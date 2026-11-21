//! Shopping Sessions pipeline. See README.md for the design.

#[cfg(feature = "diag")]
mod diag;
mod pq;
mod prof;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 604_800_000;
// Partition counts (log2), chosen at startup from the input size: event-id
// partitions for dedup, person partitions for sessions.
static PA_BITS_V: AtomicU32 = AtomicU32::new(10);
static PC_BITS_V: AtomicU32 = AtomicU32::new(10);
#[inline(always)]
fn pa_bits() -> u32 {
    PA_BITS_V.load(Ordering::Relaxed)
}
#[inline(always)]
fn pc_bits() -> u32 {
    PC_BITS_V.load(Ordering::Relaxed)
}
const MAX_PC_BITS: u32 = 10;
const NT_STORES: bool = true;
fn chunk_size() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| std::env::var("SS_CHUNK_KB").ok().and_then(|v| v.parse::<u64>().ok()).map(|k| k << 10).unwrap_or(4 << 20))
}
const PAD: usize = 128;

/// Experiment switches of the diagnostic build: 0 = SS_SF (last record of a
/// line from registers), 1 = SS_NT (streaming stores), 2 = SS_PF (input
/// prefetch distance in bytes, 0 = none).
fn knob(i: usize) -> i64 {
    static V: OnceLock<[i64; 3]> = OnceLock::new();
    V.get_or_init(|| {
        let g = |k: &str, d: i64| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d);
        [g("SS_SF", 1), g("SS_NT", 1), g("SS_PF", 0)]
    })[i]
}

// ---------------------------------------------------------------------------
// Memory helpers

extern "C" {
    fn madvise(addr: *mut std::ffi::c_void, len: usize, advice: i32) -> i32;
    fn getrusage(who: i32, usage: *mut [i64; 18]) -> i32;
    fn mmap(addr: *mut std::ffi::c_void, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut std::ffi::c_void;
    fn getrlimit(resource: i32, rlim: *mut [u64; 2]) -> i32;
    fn setrlimit(resource: i32, rlim: *const [u64; 2]) -> i32;
}

/// Every input file stays open while it is mapped: lift the soft limit on
/// open files to the hard one.
fn raise_fd_limit() {
    let mut r = [0u64; 2];
    unsafe {
        if getrlimit(7 /* RLIMIT_NOFILE */, &mut r) == 0 && r[0] < r[1] {
            r[0] = r[1];
            setrlimit(7, &r);
        }
    }
}

/// A zero-filled Vec whose pages are not touched yet, advised to use huge pages.
fn zeroed_slice<T>(n: usize) -> &'static mut [T] {
    // A fresh anonymous mapping: zero pages, faulted in lazily (in parallel by
    // whoever touches them). The system allocator would memset large
    // over-aligned blocks on one thread.
    let n = n.max(1);
    let bytes = (std::mem::size_of::<T>() * n + 4095) & !4095;
    const HP: usize = 2 << 20;
    unsafe {
        if bytes >= HP && thp_mask() & 2 != 0 {
            // 2 MB aligned so the whole array can be backed by huge pages
            let p = mmap(std::ptr::null_mut(), bytes + HP, 3, 0x02 | 0x20 | 0x4000, -1, 0);
            assert!(p as isize != -1 && !p.is_null(), "mmap failed");
            let a = (p as usize + HP - 1) & !(HP - 1);
            madvise(a as *mut _, (bytes + HP - 1) & !(HP - 1), 14 /* MADV_HUGEPAGE */);
            return std::slice::from_raw_parts_mut(a as *mut T, n);
        }
        let p = mmap(std::ptr::null_mut(), bytes, 3, 0x02 | 0x20 | 0x4000, -1, 0);
        assert!(p as isize != -1 && !p.is_null(), "mmap failed");
        std::slice::from_raw_parts_mut(p as *mut T, n)
    }
}

/// Huge-page advice (bit 0: arena, bit 1: big tables). On by default: on the
/// scorer (a VM, THP in madvise mode) a huge-page fault costs a third of the
/// 4 KB faults it replaces and random reads into the big tables halve.
fn thp_mask() -> u32 {
    static V: OnceLock<u32> = OnceLock::new();
    *V.get_or_init(|| std::env::var("SS_THP").ok().and_then(|v| v.parse().ok()).unwrap_or(3))
}

/// One big lazily-backed region split into per-thread sub-arenas, each
/// bump-allocated and freed in one call at the end: record buffers live here,
/// so building them never calls mmap/munmap, and releasing them costs one
/// MADV_DONTNEED (one TLB shootdown) per thread instead of thousands.
struct Arena {
    base: usize,
    sub: usize,
    used: Vec<AtomicUsize>,
    /// Per sub-arena, a range of dead (already faulted) memory to hand out
    /// before fresh memory: [recycle_next, recycle_end).
    recycle_next: Vec<AtomicUsize>,
    recycle_end: Vec<AtomicUsize>,
}

/// Sub-arenas: one per worker, the last one for the main thread. Set before
/// the first allocation.
static SUBS: AtomicUsize = AtomicUsize::new(33);

static ARENA: OnceLock<Arena> = OnceLock::new();

thread_local! {
    static SUB: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
}

fn arena() -> &'static Arena {
    ARENA.get_or_init(|| {
        let subs = SUBS.load(Ordering::Relaxed);
        // Untouched pages cost nothing; reserve generously, smaller if the
        // kernel refuses (strict overcommit).
        for gb in [256usize, 128, 64, 32, 20, 16, 12, 8, 6, 4] {
            let cap = gb << 30;
            let p = unsafe { mmap(std::ptr::null_mut(), cap, 3, 0x02 | 0x20 | 0x4000, -1, 0) };
            if p as isize != -1 && !p.is_null() {
                let base = (p as usize + (2 << 20) - 1) & !((2 << 20) - 1);
                let sub = ((cap - (2 << 20)) / subs) & !((2 << 20) - 1);
                if thp_mask() & 1 != 0 {
                    unsafe { madvise(base as *mut _, sub * subs, 14 /* MADV_HUGEPAGE */) };
                }
                let z = || (0..subs).map(|_| AtomicUsize::new(0)).collect();
                return Arena { base, sub, used: z(), recycle_next: z(), recycle_end: z() };
            }
        }
        panic!("cannot reserve arena");
    })
}

#[inline]
fn arena_alloc(bytes: usize) -> *mut u8 {
    let a = arena();
    let s = SUB.with(|c| c.get()).min(a.used.len() - 1);
    let bytes = (bytes + 63) & !63;
    let rn = a.recycle_next[s].load(Ordering::Relaxed);
    if rn + bytes <= a.recycle_end[s].load(Ordering::Relaxed) {
        a.recycle_next[s].store(rn + bytes, Ordering::Relaxed);
        return rn as *mut u8;
    }
    let off = a.used[s].fetch_add(bytes, Ordering::Relaxed);
    assert!(off + bytes <= a.sub, "arena exhausted");
    (a.base + s * a.sub + off) as *mut u8
}

/// Lets sub-arena `s` hand out [from, to) again (memory that is dead now).
fn recycle(s: usize, from: usize, to: usize) {
    let a = arena();
    a.recycle_next[s].store((from + 63) & !63, Ordering::Relaxed);
    a.recycle_end[s].store(to, Ordering::Relaxed);
}

/// Frees everything the calling thread's sub-arena handed out.
fn release_sub_arena(s: usize) {
    let a = arena();
    let used = a.used[s].load(Ordering::Relaxed);
    release((a.base + s * a.sub) as *const u8, used);
}

/// Drops the pages fully inside [p, p + len) (MADV_DONTNEED, which only takes
/// the mmap lock for reading, so threads can do it in parallel).
fn release(p: *const u8, len: usize) {
    let a = (p as usize + 4095) & !4095;
    let b = (p as usize + len) & !4095;
    if b > a {
        unsafe { madvise(a as *mut _, b - a, 4 /* MADV_DONTNEED */) };
    }
}

/// A Vec backed by the calling thread's arena. It must never grow past `cap`
/// (that would hand arena memory to the allocator) and is never dropped.
fn arena_vec<T>(cap: usize) -> std::mem::ManuallyDrop<Vec<T>> {
    let cap = cap.max(1);
    let p = arena_alloc(cap * std::mem::size_of::<T>()) as *mut T;
    std::mem::ManuallyDrop::new(unsafe { Vec::from_raw_parts(p, 0, cap) })
}

fn release_vec<T>(v: &Vec<T>) {
    release(v.as_ptr() as *const u8, v.capacity() * std::mem::size_of::<T>());
}

/// Growable buffer of Copy records in the arena.
pub struct PVec<T: Copy> {
    ptr: *mut T,
    len: usize,
    cap: usize,
}

unsafe impl<T: Copy> Send for PVec<T> {}
unsafe impl<T: Copy> Sync for PVec<T> {}

impl<T: Copy> PVec<T> {
    fn with_capacity(n: usize) -> Self {
        let n = n.max(16);
        PVec { ptr: arena_alloc(n * std::mem::size_of::<T>()) as *mut T, len: 0, cap: n }
    }
    #[inline(always)]
    fn push(&mut self, v: T) {
        if self.len == self.cap {
            self.grow();
        }
        unsafe { self.ptr.add(self.len).write(v) };
        self.len += 1;
    }
    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let nc = self.cap * 2;
        let np = arena_alloc(nc * std::mem::size_of::<T>()) as *mut T;
        unsafe { std::ptr::copy_nonoverlapping(self.ptr, np, self.len) };
        if thp_mask() & 1 == 0 {
            // (with huge pages a partial release would split them)
            release(self.ptr as *const u8, self.cap * std::mem::size_of::<T>());
        }
        self.ptr = np;
        self.cap = nc;
    }
    #[inline(always)]
    fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
    fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    fn extend_from_slice(&mut self, s: &[T]) {
        while self.len + s.len() > self.cap {
            self.grow();
        }
        unsafe { std::ptr::copy_nonoverlapping(s.as_ptr(), self.ptr.add(self.len), s.len()) };
        self.len += s.len();
    }
}

/// Partitioned output with software write-combining: records are staged in
/// small per-partition buffers and written out K at a time with streaming
/// stores (K * size_of::<T>() is a multiple of 64 bytes).
struct Parts<T: Copy, const K: usize> {
    out: Vec<PVec<T>>,
    buf: Vec<[T; K]>,
    n: Vec<u32>,
    sf: bool,
    nt: bool,
}

impl<T: Copy, const K: usize> Parts<T, K> {
    fn new(parts: usize, reserve: usize) -> Self {
        assert!(K * std::mem::size_of::<T>() % 64 == 0);
        let reserve = reserve.div_ceil(K) * K;
        let mut buf = Vec::with_capacity(parts);
        unsafe {
            buf.set_len(parts);
            std::ptr::write_bytes(buf.as_mut_ptr(), 0, parts);
        }
        Parts { out: (0..parts).map(|_| PVec::with_capacity(reserve)).collect(), buf, n: vec![0; parts], sf: knob(0) != 0, nt: knob(1) != 0 }
    }
    /// The index the next record pushed to partition q will get.
    #[inline(always)]
    fn next_index(&self, q: usize) -> usize {
        unsafe { self.out.get_unchecked(q).len + *self.n.get_unchecked(q) as usize }
    }
    #[inline(always)]
    fn push(&mut self, q: usize, v: T) {
        unsafe {
            let k = *self.n.get_unchecked(q) as usize;
            if k + 1 == K {
                #[cfg(target_feature = "avx2")]
                if self.sf && (std::mem::size_of::<T>() == 16 || std::mem::size_of::<T>() == 32) {
                    self.flush_with(q, v);
                    *self.n.get_unchecked_mut(q) = 0;
                    return;
                }
                *self.buf.get_unchecked_mut(q).get_unchecked_mut(k) = v;
                self.flush(q);
                *self.n.get_unchecked_mut(q) = 0;
            } else {
                *self.buf.get_unchecked_mut(q).get_unchecked_mut(k) = v;
                *self.n.get_unchecked_mut(q) = (k + 1) as u32;
            }
        }
    }
    /// Writes out partition q's staged K - 1 records followed by `v`. The last
    /// record goes from registers straight into the output vector: storing it
    /// in the staging buffer and loading the whole line back would make the
    /// load wait for the store to leave the store buffer (a wide load cannot
    /// be forwarded from narrower stores), behind every older pending store.
    #[cfg(target_feature = "avx2")]
    #[inline(always)]
    unsafe fn flush_with(&mut self, q: usize, v: T) {
        use std::arch::x86_64::*;
        let nt = self.nt;
        let o = self.out.get_unchecked_mut(q);
        if o.len + K > o.cap {
            o.grow();
        }
        let dst = o.ptr.add(o.len) as *mut u8;
        let src = self.buf.get_unchecked(q).as_ptr() as *const u8;
        let bytes = K * std::mem::size_of::<T>();
        let mut i = 0;
        while i + 32 < bytes {
            let x = _mm256_loadu_si256(src.add(i) as *const __m256i);
            if nt {
                _mm256_stream_si256(dst.add(i) as *mut __m256i, x);
            } else {
                _mm256_storeu_si256(dst.add(i) as *mut __m256i, x);
            }
            i += 32;
        }
        let last = if std::mem::size_of::<T>() == 16 {
            let w = std::mem::transmute_copy::<T, [u64; 2]>(&v);
            let lo = _mm_loadu_si128(src.add(i) as *const __m128i);
            _mm256_inserti128_si256(_mm256_castsi128_si256(lo), _mm_set_epi64x(w[1] as i64, w[0] as i64), 1)
        } else {
            let w = std::mem::transmute_copy::<T, [u64; 4]>(&v);
            _mm256_set_epi64x(w[3] as i64, w[2] as i64, w[1] as i64, w[0] as i64)
        };
        if nt {
            _mm256_stream_si256(dst.add(i) as *mut __m256i, last);
        } else {
            _mm256_storeu_si256(dst.add(i) as *mut __m256i, last);
        }
        o.len += K;
    }
    #[inline(always)]
    fn flush(&mut self, q: usize) {
        unsafe {
            let nt = self.nt;
            let o = self.out.get_unchecked_mut(q);
            if o.len + K > o.cap {
                o.grow();
            }
            let dst = o.ptr.add(o.len) as *mut u8;
            let src = self.buf.get_unchecked(q).as_ptr() as *const u8;
            let bytes = K * std::mem::size_of::<T>();
            #[cfg(target_feature = "avx2")]
            {
                use std::arch::x86_64::*;
                let mut i = 0;
                while i < bytes {
                    let v = _mm256_loadu_si256(src.add(i) as *const __m256i);
                    if NT_STORES && nt {
                        _mm256_stream_si256(dst.add(i) as *mut __m256i, v);
                    } else {
                        _mm256_storeu_si256(dst.add(i) as *mut __m256i, v);
                    }
                    i += 32;
                }
            }
            #[cfg(not(target_feature = "avx2"))]
            std::ptr::copy_nonoverlapping(src, dst, bytes);
            o.len += K;
        }
    }
    /// Writes out the staged remainders and returns the partitions.
    fn finish(mut self) -> Vec<PVec<T>> {
        for q in 0..self.out.len() {
            let k = self.n[q] as usize;
            for j in 0..k {
                let v = self.buf[q][j];
                self.out[q].push(v);
            }
        }
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_mm_sfence();
        }
        self.out
    }
}

/// Prefetch with intent to write (PREFETCHW; a no-op hint where unsupported).
#[inline(always)]
fn prefetch_w<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::asm!("prefetchw [{0}]", in(reg) p, options(nostack, preserves_flags, readonly));
    }
}

#[inline(always)]
fn prefetch<T>(p: *const T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0);
    }
}

// ---------------------------------------------------------------------------
// Keys: strings of up to 15 bytes are packed exactly (big-endian bytes, length
// in the last byte) so comparing keys compares strings byte-wise. Longer
// strings get a 120-bit hash with 0xFF in the last byte and are remembered in
// LONG so they can be printed and compared.

type Key = [u64; 2];
const ZERO: Key = [0, 0];

/// Strings of more than 15 bytes, by their hashed key: sharded, and each
/// thread remembers which keys it already registered.
const LONG_SHARDS: usize = 64;
static LONG: [Mutex<BTreeMap<Key, Vec<u8>>>; LONG_SHARDS] = [const { Mutex::new(BTreeMap::new()) }; LONG_SHARDS];

thread_local! {
    static LONG_SEEN: std::cell::RefCell<std::collections::HashSet<Key>> = std::cell::RefCell::new(std::collections::HashSet::new());
}

#[inline]
fn long_shard(k: &Key) -> &'static Mutex<BTreeMap<Key, Vec<u8>>> {
    &LONG[(k[0] >> 58) as usize % LONG_SHARDS]
}

#[inline(always)]
fn fm(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[cold]
#[inline(never)]
fn long_key(s: &[u8]) -> Key {
    let mut h1: u64 = 0x243F_6A88_85A3_08D3 ^ s.len() as u64;
    let mut h2: u64 = 0x1319_8A2E_0370_7344;
    for c in s.chunks(8) {
        let mut w = [0u8; 8];
        w[..c.len()].copy_from_slice(c);
        let x = u64::from_le_bytes(w);
        h1 = fm(h1 ^ x, 0x9E37_79B9_7F4A_7C15);
        h2 = fm(h2 ^ x, 0xC2B2_AE3D_27D4_EB4F) ^ h1;
    }
    let k = [h1, (h2 & !0xFF) | 0xFF];
    let fresh = LONG_SEEN.with(|seen| seen.borrow_mut().insert(k));
    if fresh {
        long_shard(&k).lock().unwrap().entry(k).or_insert_with(|| s.to_vec());
    }
    k
}

/// Every slice passed here must have at least 16 readable bytes from its start
/// (all buffers carry PAD bytes of slack).
#[inline(always)]
fn key_of(s: &[u8]) -> Key {
    let n = s.len();
    if n > 15 {
        return long_key(s);
    }
    // the first n bytes, big-endian, zero-padded; n in the last byte
    let lo = unsafe { (s.as_ptr() as *const u64).read_unaligned() };
    let hi = unsafe { (s.as_ptr().add(8) as *const u64).read_unaligned() };
    // KEEP[n]: masks of the first n bytes of the two words
    static KEEP: [[u64; 2]; 16] = {
        let mut t = [[0u64; 2]; 16];
        let mut n = 1;
        while n < 16 {
            t[n] = if n >= 8 { [!0, if n == 8 { 0 } else { !(!0u64 << (8 * n - 64)) }] } else { [!(!0u64 << (8 * n)), 0] };
            n += 1;
        }
        t
    };
    let m = unsafe { KEEP.get_unchecked(n) };
    [(lo & m[0]).swap_bytes(), (hi & m[1]).swap_bytes() | n as u64]
}

#[inline(always)]
fn is_short(k: &Key) -> bool {
    (k[1] & 0xFF) <= 15
}

fn key_bytes(k: &Key, out: &mut Vec<u8>) {
    if is_short(k) {
        let n = (k[1] & 0xFF) as usize;
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&k[0].to_be_bytes());
        b[8..].copy_from_slice(&k[1].to_be_bytes());
        out.extend_from_slice(&b[..n]);
    } else {
        out.extend_from_slice(&long_shard(k).lock().unwrap()[k]);
    }
}

/// Byte-wise string order.
#[inline(always)]
fn cmp_key(a: &Key, b: &Key) -> std::cmp::Ordering {
    if is_short(a) && is_short(b) {
        a.cmp(b)
    } else {
        cmp_key_slow(a, b)
    }
}

#[cold]
fn cmp_key_slow(a: &Key, b: &Key) -> std::cmp::Ordering {
    let mut x = Vec::new();
    let mut y = Vec::new();
    key_bytes(a, &mut x);
    key_bytes(b, &mut y);
    x.cmp(&y)
}

#[inline(always)]
fn hk(k: &Key) -> u64 {
    fm(fm(k[0] ^ 0x9E37_79B9_7F4A_7C15, 0xD6E8_FEB8_6659_FD93) ^ k[1], 0xA076_1D64_78BD_642F)
}

/// Open-addressing map from non-empty keys; fixed capacity.
struct KeyMap<V: Copy + Default> {
    keys: Vec<Key>,
    vals: Vec<V>,
    mask: usize,
}

impl<V: Copy + Default> KeyMap<V> {
    fn new(n: usize) -> Self {
        let size = (n * 2).next_power_of_two().max(16);
        KeyMap { keys: vec![ZERO; size], vals: vec![V::default(); size], mask: size - 1 }
    }
    #[inline(always)]
    fn get(&self, k: &Key) -> Option<V> {
        let mut i = hk(k) as usize & self.mask;
        loop {
            let kk = unsafe { self.keys.get_unchecked(i) };
            if kk == k {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if *kk == ZERO {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Returns the slot's value, inserting `v` if absent; bool = inserted.
    fn get_or_insert(&mut self, k: &Key, v: V) -> (V, bool) {
        let mut i = hk(k) as usize & self.mask;
        loop {
            let kk = self.keys[i];
            if kk == *k {
                return (self.vals[i], false);
            }
            if kk == ZERO {
                self.keys[i] = *k;
                self.vals[i] = v;
                return (v, true);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------------------
// Link map: every linked ID -> the name (smallest ID) of its person.

#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct Slot {
    k: Key,
    v: Key,
}

struct LinkMap {
    slots: *mut Slot,
    mask: usize,
    /// Build-time state: parent (slot + 1, 0 = unclaimed).
    parent: &'static [AtomicU32],
    /// One bit per hash (top 24 bits) of every ID whose person is not itself:
    /// a clear bit means "the person is the ID", with no table probe.
    filter: &'static [AtomicU64],
}

const FILTER_BITS: u32 = 24;

unsafe impl Sync for LinkMap {}
unsafe impl Send for LinkMap {}

impl LinkMap {
    fn with_pairs(pairs: usize) -> LinkMap {
        let occ = pairs * 2;
        let size = (occ + 1).next_power_of_two().max(16);
        let slots = zeroed_slice::<Slot>(size).as_mut_ptr();
        LinkMap {
            slots,
            mask: size - 1,
            parent: zeroed_slice(size),
            filter: zeroed_slice(1 << (FILTER_BITS - 6)),
        }
    }
    fn size(&self) -> usize {
        self.mask + 1
    }
    #[inline(always)]
    fn slot_of(&self, h: u64) -> usize {
        h as usize & self.mask
    }
    #[inline(always)]
    fn slot(&self, i: usize) -> &Slot {
        unsafe { &*self.slots.add(i) }
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        let i = self.slot_of(h);
        prefetch(unsafe { self.slots.add(i) });
        prefetch(unsafe { self.slots.add((i + 2) & self.mask) });
    }
    #[inline(always)]
    fn prefetch_filter(&self, h: u64) {
        let b = h >> (64 - FILTER_BITS);
        prefetch(unsafe { self.filter.as_ptr().add((b >> 6) as usize) });
    }
    /// False when user hash `h` surely maps to itself.
    #[inline(always)]
    fn maybe_linked(&self, h: u64) -> bool {
        let b = h >> (64 - FILTER_BITS);
        let w = unsafe { self.filter.get_unchecked((b >> 6) as usize) }.load(Ordering::Relaxed);
        (w >> (b & 63)) & 1 != 0
    }
    /// The person for user `k` (hash `h`).
    #[inline(always)]
    fn person(&self, k: &Key, h: u64) -> Key {
        let mut i = self.slot_of(h);
        loop {
            let s = self.slot(i);
            if s.k == *k {
                return s.v;
            }
            if s.k == ZERO {
                return *k;
            }
            i = (i + 1) & self.mask;
        }
    }

    // --- concurrent build ---------------------------------------------------

    #[inline(always)]
    fn k_atomic(&self, i: usize) -> (&AtomicU64, &AtomicU64) {
        unsafe {
            let p = self.slots.add(i) as *mut u64;
            (AtomicU64::from_ptr(p), AtomicU64::from_ptr(p.add(1)))
        }
    }

    /// Claims or finds the slot of key `k` (non-empty, so k[1] != 0).
    #[inline(always)]
    fn insert(&self, k: &Key, h: u64) -> u32 {
        let mut i = self.slot_of(h);
        loop {
            let (a0, a1) = self.k_atomic(i);
            let s1 = a1.load(Ordering::Acquire);
            if s1 == 0 {
                if a1.compare_exchange(0, k[1], Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    a0.store(k[0], Ordering::Relaxed);
                    self.parent[i].store(i as u32 + 1, Ordering::Release);
                    return i as u32;
                }
                continue;
            }
            if s1 == k[1] {
                while self.parent[i].load(Ordering::Acquire) == 0 {
                    std::hint::spin_loop();
                }
                if a0.load(Ordering::Relaxed) == k[0] {
                    return i as u32;
                }
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Ordering::Acquire) - 1;
            if p == x {
                return x;
            }
            let gp = self.parent[p as usize].load(Ordering::Acquire) - 1;
            if gp != p {
                let _ = self.parent[x as usize].compare_exchange_weak(p + 1, gp + 1, Ordering::AcqRel, Ordering::Relaxed);
            }
            x = gp;
        }
    }

    fn union(&self, a: u32, b: u32) {
        let (mut a, mut b) = (a, b);
        loop {
            a = self.find(a);
            b = self.find(b);
            if a == b {
                return;
            }
            // the root is always the byte-wise smallest key, i.e. the name
            let (hi, lo) = if cmp_key(&self.slot(a as usize).k, &self.slot(b as usize).k).is_gt() { (a, b) } else { (b, a) };
            if self.parent[hi as usize]
                .compare_exchange(hi + 1, lo + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Inserts and unions a batch of pairs, with prefetching.
    fn add_pairs(&self, pairs: &[(Key, Key)]) {
        const B: usize = 16;
        let mut ha = [0u64; B];
        let mut hb = [0u64; B];
        for chunk in pairs.chunks(B) {
            for (j, (a, b)) in chunk.iter().enumerate() {
                ha[j] = hk(a);
                hb[j] = hk(b);
                for h in [ha[j], hb[j]] {
                    let i = self.slot_of(h);
                    prefetch_w(unsafe { self.slots.add(i) });
                    prefetch_w(unsafe { self.parent.as_ptr().add(i) });
                }
            }
            // claim the slots, then bring in the roots' keys (the union compares
            // them) before linking
            let mut xs = [0u32; B];
            let mut ys = [0u32; B];
            for (j, (a, b)) in chunk.iter().enumerate() {
                let x = self.find(self.insert(a, ha[j]));
                let y = self.find(self.insert(b, hb[j]));
                prefetch(unsafe { self.slots.add(x as usize) });
                prefetch(unsafe { self.slots.add(y as usize) });
                xs[j] = x;
                ys[j] = y;
            }
            for j in 0..chunk.len() {
                self.union(xs[j], ys[j]);
            }
        }
    }

    /// For the slots in [from, to): store the person name as the value.
    fn finish(&self, from: usize, to: usize) {
        // in batches: find the roots (parent array only) and prefetch their
        // slots and the filter words, then copy the names
        const B: usize = 32;
        let mut idx = [0usize; B];
        let mut root = [0usize; B];
        let mut fbit = [0u64; B];
        let mut n = 0usize;
        let write = |idx: &[usize], root: &[usize], fbit: &[u64]| {
            for j in 0..idx.len() {
                let (i, b) = (idx[j], root[j]);
                let name = self.slot(b).k;
                unsafe { (*self.slots.add(i)).v = name };
                if b != i {
                    let fb = fbit[j];
                    self.filter[(fb >> 6) as usize].fetch_or(1 << (fb & 63), Ordering::Relaxed);
                }
            }
        };
        for i in from..to {
            if self.parent[i].load(Ordering::Relaxed) == 0 {
                continue;
            }
            let b = self.find(i as u32) as usize;
            idx[n] = i;
            root[n] = b;
            if b != i {
                prefetch(unsafe { self.slots.add(b) });
                let fb = hk(&self.slot(i).k) >> (64 - FILTER_BITS);
                prefetch_w(unsafe { self.filter.as_ptr().add((fb >> 6) as usize) });
                fbit[n] = fb;
            }
            n += 1;
            if n == B {
                write(&idx, &root, &fbit);
                n = 0;
            }
        }
        write(&idx[..n], &root[..n], &fbit[..n]);
    }
}

// ---------------------------------------------------------------------------
// Scanning

#[inline(always)]
fn find_byte(buf: &[u8], from: usize, c: u8) -> usize {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        let n = buf.len();
        let mut i = from;
        let needle = _mm256_set1_epi8(c as i8);
        while i + 32 <= n {
            let v = _mm256_loadu_si256(buf.as_ptr().add(i) as *const __m256i);
            let m = _mm256_movemask_epi8(_mm256_cmpeq_epi8(v, needle)) as u32;
            if m != 0 {
                return i + m.trailing_zeros() as usize;
            }
            i += 32;
        }
        while i < n {
            if *buf.get_unchecked(i) == c {
                return i;
            }
            i += 1;
        }
        n
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        match buf[from..].iter().position(|&b| b == c) {
            Some(p) => from + p,
            None => buf.len(),
        }
    }
}

/// Bitmask of ',', '\n' and '"' in the 64 bytes at p.
#[inline(always)]
unsafe fn block_mask(p: *const u8) -> u64 {
    #[cfg(target_feature = "avx2")]
    {
        use std::arch::x86_64::*;
        // low-nibble lookup: 0x2 -> '"', 0xA -> '\n', 0xC -> ','; 0 -> 0x01 (never matches)
        let table = _mm256_setr_epi8(
            1, 0, 0x22, 0, 0, 0, 0, 0, 0, 0, 0x0A, 0, 0x2C, 0, 0, 0, 1, 0, 0x22, 0, 0, 0, 0, 0, 0, 0, 0x0A, 0, 0x2C, 0, 0,
            0,
        );
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let ma = _mm256_cmpeq_epi8(_mm256_shuffle_epi8(table, a), a);
        let mb = _mm256_cmpeq_epi8(_mm256_shuffle_epi8(table, b), b);
        (_mm256_movemask_epi8(ma) as u32 as u64) | ((_mm256_movemask_epi8(mb) as u32 as u64) << 32)
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let mut m = 0u64;
        for i in 0..64 {
            let c = *p.add(i);
            if c == b',' || c == b'\n' || c == b'"' {
                m |= 1 << i;
            }
        }
        m
    }
}

struct Scan {
    base: usize,
    bits: u64,
}

impl Scan {
    #[inline(always)]
    fn next(&mut self, ptr: *const u8) -> usize {
        while self.bits == 0 {
            self.base += 64;
            self.bits = unsafe { block_mask(ptr.add(self.base)) };
        }
        let p = self.base + self.bits.trailing_zeros() as usize;
        self.bits &= self.bits - 1;
        p
    }
}

/// Calls `f` with the N field values of every well-formed line starting in
/// [begin, limit). `buf` must end with '\n' and have PAD readable bytes after.
#[inline(always)]
fn scan_lines<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(buf: &'a [u8], begin: usize, limit: usize, mut f: F) {
    let n = buf.len();
    if begin >= n {
        return;
    }
    debug_assert!(buf[n - 1] == b'\n');
    let ptr = buf.as_ptr();
    let at = |i: usize| unsafe { *ptr.add(i) };
    let base0 = begin & !63;
    let mut sc = Scan { base: base0, bits: unsafe { block_mask(ptr.add(base0)) } & (!0u64 << (begin - base0)) };
    let mut fields: [&'a [u8]; N] = [&[]; N];
    let mut ls = begin;
    let skip = |sc: &mut Scan, mut p: usize| -> usize {
        while at(p) != b'\n' {
            p = sc.next(ptr);
        }
        p
    };
    while ls < limit && ls < n {
        let mut start = ls;
        let mut k = 0;
        let ok;
        let end;
        loop {
            let mut p = sc.next(ptr);
            let mut c = at(p);
            let (vs, ve);
            if c == b'"' {
                if p != start {
                    end = skip(&mut sc, p);
                    ok = false;
                    break;
                }
                let p2 = sc.next(ptr);
                if at(p2) != b'"' {
                    end = skip(&mut sc, p2);
                    ok = false;
                    break;
                }
                let p3 = sc.next(ptr);
                c = at(p3);
                if p3 != p2 + 1 || c == b'"' {
                    end = skip(&mut sc, p3);
                    ok = false;
                    break;
                }
                vs = p + 1;
                ve = p2;
                p = p3;
            } else {
                vs = start;
                ve = p;
            }
            if k == N {
                end = skip(&mut sc, p);
                ok = false;
                break;
            }
            fields[k] = unsafe { buf.get_unchecked(vs..ve) };
            k += 1;
            if c == b'\n' {
                end = p;
                ok = k == N;
                break;
            }
            start = p + 1;
        }
        if ok {
            f(&fields);
        }
        ls = end + 1;
    }
}

// --- line-at-a-time scanner ---------------------------------------------------
// One line per step, from the masks of the 128 bytes at its start: its end is
// the first newline; it has N fields iff the masks hold N separators up to and
// including that newline; quotes are checked and stripped with bit tricks
// (PEXT lines the quote bits up with the fields). Lines of 128 bytes or more
// take the plain scanner.

/// (separators, newlines, quotes) in the 64 bytes at p.
#[inline(always)]
unsafe fn block_masks3(p: *const u8) -> (u64, u64, u64) {
    #[cfg(target_feature = "avx2")]
    {
        use std::arch::x86_64::*;
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let (nl, cm, qt) = (_mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'"' as i8));
        let (na, nb) = (_mm256_cmpeq_epi8(a, nl), _mm256_cmpeq_epi8(b, nl));
        let (sa, sb) = (_mm256_or_si256(na, _mm256_cmpeq_epi8(a, cm)), _mm256_or_si256(nb, _mm256_cmpeq_epi8(b, cm)));
        let (qa, qb) = (_mm256_cmpeq_epi8(a, qt), _mm256_cmpeq_epi8(b, qt));
        let m = |x: __m256i, y: __m256i| (_mm256_movemask_epi8(x) as u32 as u64) | ((_mm256_movemask_epi8(y) as u32 as u64) << 32);
        (m(sa, sb), m(na, nb), m(qa, qb))
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let (mut s, mut n, mut q) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let c = *p.add(i);
            s |= ((c == b',' || c == b'\n') as u64) << i;
            n |= ((c == b'\n') as u64) << i;
            q |= ((c == b'"') as u64) << i;
        }
        (s, n, q)
    }
}

#[inline(always)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    let (s0, n0, q0) = block_masks3(p);
    let (s1, n1, q1) = block_masks3(p.add(64));
    ((s0 as u128) | ((s1 as u128) << 64), (n0 as u128) | ((n1 as u128) << 64), (q0 as u128) | ((q1 as u128) << 64))
}

/// The bits of `v` selected by `m`, packed to the low end.
#[inline(always)]
fn pext128(v: u128, m: u128) -> u64 {
    #[cfg(target_feature = "bmi2")]
    unsafe {
        use std::arch::x86_64::_pext_u64;
        let lo = _pext_u64(v as u64, m as u64);
        let hi = _pext_u64((v >> 64) as u64, (m >> 64) as u64);
        lo | (hi << ((m as u64).count_ones() & 63))
    }
    #[cfg(not(target_feature = "bmi2"))]
    {
        let (mut m, mut out, mut k) = (m, 0u64, 0);
        while m != 0 {
            let b = m.trailing_zeros();
            out |= (((v >> b) & 1) as u64) << k;
            k += 1;
            m &= m - 1;
        }
        out
    }
}

/// One line with the plain scanner (lines too long for the masks).
#[cold]
#[inline(never)]
fn slow_line<'a, const N: usize>(buf: &'a [u8], ls: usize) -> (usize, Option<[&'a [u8]; N]>) {
    let e = find_byte(buf, ls, b'\n');
    let mut out = None;
    scan_lines::<N, _>(&buf[..e + 1], ls, e + 1, |f| out = Some(*f));
    (e, out)
}

/// Same contract as `scan_lines`; `buf` needs 128 readable bytes past its end
/// and `begin` must be a line start.
#[inline(always)]
fn scan_fast<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(buf: &'a [u8], begin: usize, limit: usize, mut f: F) {
    let n = buf.len();
    if begin >= n {
        return;
    }
    if option_env!("SS_NEWSCAN").is_none() {
        return scan_lines::<N, F>(buf, begin, limit, f);
    }
    debug_assert!(buf[n - 1] == b'\n');
    debug_assert!(N >= 1 && N <= 16);
    let ptr = buf.as_ptr();
    let mut ls = begin;
    let mut fields: [&'a [u8]; N] = [&[]; N];
    while ls < limit && ls < n {
        let (sep, nl, qt) = if option_env!("SS_ALIGNED").is_some() {
            // aligned blocks, shifted to the line start
            let o = (ls & 63) as u32;
            let b = unsafe { ptr.add(ls & !63) };
            let (s0, n0, q0) = unsafe { block_masks3(b) };
            let (s1, n1, q1) = unsafe { block_masks3(b.add(64)) };
            let (s2, n2, q2) = unsafe { block_masks3(b.add(128)) };
            let w = |a: u64, b: u64, c: u64| -> u128 {
                let lo = (a >> o) | ((b << 1) << (63 - o));
                let hi = (b >> o) | ((c << 1) << (63 - o));
                (lo as u128) | ((hi as u128) << 64)
            };
            (w(s0, s1, s2), w(n0, n1, n2), w(q0, q1, q2))
        } else {
            unsafe { masks128(ptr.add(ls)) }
        };
        if nl == 0 {
            let (e, r) = slow_line::<N>(buf, ls);
            if let Some(r) = r {
                f(&r);
            }
            ls = e + 1;
            continue;
        }
        let e = nl.trailing_zeros() as usize;
        let keep = 2u128.wrapping_shl(e as u32).wrapping_sub(1);
        let sep = sep & keep;
        let qt = qt & keep;
        // positions right after a separator (or at the line start) / right before one
        let after = (sep << 1) | 1;
        let before = sep >> 1;
        let opens = pext128(qt, after);
        let closes = pext128(qt << 1, sep);
        let bad = (qt & !(after ^ before)) != 0 || opens != closes || sep.count_ones() as usize != N;
        if !bad {
            // separator positions: those of the low word, then those of the high word
            let mut pos = [0u32; 32];
            let (mut lo, mut hi) = (sep as u64, (sep >> 64) as u64);
            let c = lo.count_ones() as usize;
            unsafe {
                for i in 0..N {
                    *pos.get_unchecked_mut(i) = lo.trailing_zeros();
                    lo &= lo.wrapping_sub(1);
                }
                for i in 0..N {
                    *pos.get_unchecked_mut(c + i) = 64 + hi.trailing_zeros();
                    hi &= hi.wrapping_sub(1);
                }
                let mut s = ls;
                for i in 0..N {
                    let en = ls + *pos.get_unchecked(i) as usize;
                    let q = ((opens >> i) & 1) as usize;
                    fields[i] = std::slice::from_raw_parts(ptr.add(s + q), en - s - 2 * q);
                    s = en + 1;
                }
            }
            f(&fields);
        }
        ls += e + 1;
    }
}

// --- lean line-at-a-time scanner ---------------------------------------------
// The scorer runs two hyperthreads per core flat out, so what counts is the
// number of micro-ops per line. One line per step from the masks of the bytes
// at its start; the first K separators come from the low 64 bits, the other
// N - K from the masks shifted to start right after the K-th (so each side is
// plain 64-bit arithmetic). Quotes are checked with two PEXTs per side and
// stripped by moving the field start/end bits, so a field costs two TZCNTs.
// Anything unusual (a long line, a long field) takes the plain scanner.

/// (separators, newlines, quotes) of the WIN bytes at p, as [bits 0..64, bits 64..].
#[inline(always)]
unsafe fn win_masks(p: *const u8) -> ([u64; 2], [u64; 2], [u64; 2]) {
    #[cfg(target_feature = "avx2")]
    {
        use std::arch::x86_64::*;
        let (nl, cm, qt) = (_mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'"' as i8));
        let a = _mm256_loadu_si256(p as *const __m256i);
        let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let c = _mm256_loadu_si256(p.add(64) as *const __m256i);
        let m = |x: __m256i| _mm256_movemask_epi8(x) as u32 as u64;
        let (na, nb, nc) = (_mm256_cmpeq_epi8(a, nl), _mm256_cmpeq_epi8(b, nl), _mm256_cmpeq_epi8(c, nl));
        let sa = _mm256_or_si256(na, _mm256_cmpeq_epi8(a, cm));
        let sb = _mm256_or_si256(nb, _mm256_cmpeq_epi8(b, cm));
        let sc = _mm256_or_si256(nc, _mm256_cmpeq_epi8(c, cm));
        let (qa, qb, qc) = (_mm256_cmpeq_epi8(a, qt), _mm256_cmpeq_epi8(b, qt), _mm256_cmpeq_epi8(c, qt));
        ([m(sa) | (m(sb) << 32), m(sc)], [m(na) | (m(nb) << 32), m(nc)], [m(qa) | (m(qb) << 32), m(qc)])
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let (s0, n0, q0) = block_masks3(p);
        let (s1, n1, q1) = block_masks3(p.add(64));
        ([s0, s1], [n0, n1], [q0, q1])
    }
}

#[inline(always)]
fn pext64(v: u64, m: u64) -> u64 {
    #[cfg(target_feature = "bmi2")]
    unsafe {
        std::arch::x86_64::_pext_u64(v, m)
    }
    #[cfg(not(target_feature = "bmi2"))]
    {
        pext128(v as u128, m as u128)
    }
}

#[cold]
#[inline(never)]
fn call_cold<A, F: FnMut(&A)>(f: &mut F, a: &A) {
    f(a)
}

/// Same contract as `scan_lines`; `buf` needs 128 readable bytes past its end
/// and `begin` must be a line start. 1 <= K < N.
#[inline(always)]
fn scan_lean<'a, const N: usize, const K: usize, F: FnMut(&[&'a [u8]; N])>(buf: &'a [u8], begin: usize, limit: usize, mut f: F) {
    let n = buf.len();
    if begin >= n {
        return;
    }
    debug_assert!(buf[n - 1] == b'\n');
    debug_assert!(K >= 1 && K < N);
    let ptr = buf.as_ptr();
    let mut ls = begin;
    let pf = knob(2) as usize;
    while ls < limit && ls < n {
        let p = unsafe { ptr.add(ls) };
        if pf != 0 {
            prefetch(p.wrapping_add(pf));
        }
        let (sep, nl, qt) = unsafe { win_masks(p) };
        // the line ends at the first newline (128: none in the window)
        let e = if nl[0] != 0 { nl[0].trailing_zeros() } else { 64 + nl[1].trailing_zeros() } as usize;
        // s1: the low separators without the first K - 1; r: just past the K-th
        let mut s1 = sep[0];
        for _ in 1..K {
            s1 &= s1.wrapping_sub(1);
        }
        let r = s1.trailing_zeros() as usize + 1;
        if r > 63 || e.wrapping_sub(r) >= 64 {
            let (end, res) = slow_line::<N>(buf, ls);
            if let Some(res) = res {
                call_cold(&mut f, &res);
            }
            ls = end + 1;
            continue;
        }
        // Per side: `seps` its separators, `starts` its field starts, `q` the
        // quotes. A quote must open a field (o) or close one (c), and a field
        // has both or neither: adding o to !seps carries from an opening quote
        // to the field's separator, which must then have a closing quote
        // before it. Quotes are stripped by moving the start/end bits.
        let s2 = s1 & s1.wrapping_sub(1);
        let seps = sep[0] ^ s2;
        let starts = ((sep[0] ^ s1) << 1) | 1;
        let q = qt[0] & (s1 ^ s1.wrapping_sub(1));
        let (o, c) = (q & starts, q & (seps >> 1));
        let bad_lo = (q ^ o ^ c) | (((!seps).wrapping_add(o) ^ (c << 1)) & seps);
        // high side: fields K..N, bit 0 = the byte at r
        let w = (sep[0] >> r) | (sep[1] << (64 - r));
        let mut t1 = w;
        for _ in 1..N - K {
            t1 &= t1.wrapping_sub(1);
        }
        let t2 = t1 & t1.wrapping_sub(1);
        let sepw = w ^ t2;
        let startw = ((w ^ t1) << 1) | 1;
        let qw = ((qt[0] >> r) | (qt[1] << (64 - r))) & (t1 ^ t1.wrapping_sub(1));
        let (ow, cw) = (qw & startw, qw & (sepw >> 1));
        let bad_hi = (qw ^ ow ^ cw) | (((!sepw).wrapping_add(ow) ^ (cw << 1)) & sepw);
        // well-formed: quotes only around whole fields, and the N-th separator is the newline
        if (bad_lo | bad_hi) == 0 && r + t1.trailing_zeros() as usize == e {
            // first and one-past-last content byte of each field (inside the quotes)
            let mut fields: [&'a [u8]; N] = [&[]; N];
            let (mut cs, mut ce) = (starts + o, seps - c);
            unsafe {
                for i in 0..K {
                    let (a, b) = (cs.trailing_zeros() as usize, ce.trailing_zeros() as usize);
                    *fields.get_unchecked_mut(i) = std::slice::from_raw_parts(p.add(a), b.wrapping_sub(a));
                    cs &= cs.wrapping_sub(1);
                    ce &= ce.wrapping_sub(1);
                }
                let pr = p.add(r);
                let (mut cs, mut ce) = (startw + ow, sepw - cw);
                for i in K..N {
                    let (a, b) = (cs.trailing_zeros() as usize, ce.trailing_zeros() as usize);
                    *fields.get_unchecked_mut(i) = std::slice::from_raw_parts(pr.add(a), b.wrapping_sub(a));
                    cs &= cs.wrapping_sub(1);
                    ce &= ce.wrapping_sub(1);
                }
            }
            f(&fields);
        }
        ls += e + 1;
    }
}

/// Pads a whole-file buffer: guarantees a final '\n' and PAD zero bytes.
fn pad_buffer(mut v: Vec<u8>) -> Vec<u8> {
    if v.last() != Some(&b'\n') {
        v.push(b'\n');
    }
    let n = v.len();
    v.resize(n + PAD, 0);
    v.truncate(n);
    v
}

// ---------------------------------------------------------------------------
// Parsing values

#[inline(always)]
fn dig(c: u8) -> u32 {
    c.wrapping_sub(b'0') as u32
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = dig(unsafe { *s.get_unchecked(i) });
    let b = dig(unsafe { *s.get_unchecked(i + 1) });
    if a > 9 || b > 9 {
        None
    } else {
        Some((a * 10 + b) as i64)
    }
}

/// F1 (len 20) or F2 (len 25); caller checked the length and s[4] == '-'.
#[inline(always)]
fn parse_iso(s: &[u8]) -> Option<i64> {
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    let off = if s.len() == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om)
    };
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let dim = match mo {
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    Some(((days * 86_400 + h * 3600 + mi * 60 + se) - off * 60) * 1000)
}

/// Returns epoch ms, or None for a bad timestamp (an F3 value too large for
/// any window also returns None).
#[inline(always)]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_iso(s);
    }
    let mut v: u64 = 0;
    let mut sig = 0u32;
    for &c in s {
        let d = dig(c);
        if d > 9 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig > 18 {
                return None;
            }
            v = v * 10 + d as u64;
        }
    }
    Some(v as i64)
}

// --- SWAR timestamp parsing ------------------------------------------------
// The slices passed here always have at least 24 readable bytes from their
// start (buffers carry PAD bytes of slack).

#[inline(always)]
fn load8(s: &[u8], at: usize) -> u64 {
    unsafe { (s.as_ptr().add(at) as *const u64).read_unaligned() }
}

/// For each byte lane selected by `lanes` (0xFF per lane), true if it holds an
/// ASCII digit.
#[inline(always)]
fn all_digits(v: u64, lanes: u64) -> bool {
    let t = v ^ 0x3030_3030_3030_3030;
    (((t.wrapping_add(0x7676_7676_7676_7676)) | t) & lanes & 0x8080_8080_8080_8080) == 0
}

/// Eight ASCII digits (first digit in the lowest byte) to their value.
#[inline(always)]
fn eight_digits(v: u64) -> u64 {
    let v = v.wrapping_sub(0x3030_3030_3030_3030);
    let v = (v.wrapping_mul(10) + (v >> 8)) & 0x00FF_00FF_00FF_00FF;
    let v = (v.wrapping_mul(100) + (v >> 16)) & 0x0000_FFFF_0000_FFFF;
    v.wrapping_mul(10000).wrapping_add(v >> 32) & 0xFFFF_FFFF
}


#[inline(always)]
fn days_from_parts(before: u32, leap: bool, m: u32, d: u32) -> i64 {
    const CUM: [u32; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    (before + CUM[m as usize] + (leap && m > 2) as u32 + d - 1) as i64 - 719_528
}

/// For each year 0..=9999: days from 0000-01-01 to its Jan 1, with bit 31 set
/// for leap years. Built at compile time.
static YEARS: [u32; 10000] = {
    let mut t = [0u32; 10000];
    let mut y = 0;
    while y < 10000 {
        let before = 365 * y + (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400;
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        t[y as usize] = before | ((leap as u32) << 31);
        y += 1;
    }
    t
};

#[inline(always)]
fn year_info(y: u32) -> (u32, bool) {
    let v = YEARS[y as usize];
    (v & 0x7FFF_FFFF, v >> 31 != 0)
}

/// Days since 1970-01-01 for a valid proleptic Gregorian date, y in 0..=9999.
#[allow(dead_code)]
#[inline(always)]
fn days_fast(y: u32, m: u32, d: u32, leap: bool) -> i64 {
    const CUM: [u32; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
    let before = 365 * y + (y + 3) / 4 - (y + 99) / 100 + (y + 399) / 400;
    let doy = CUM[m as usize] + (leap && m > 2) as u32 + d - 1;
    (before + doy) as i64 - 719_528
}

/// F1 (len 20) or F2 (len 25), SWAR-validated. Caller checked the length.
#[inline(always)]
fn parse_iso_fast(s: &[u8]) -> Option<i64> {
    let a = load8(s, 0); // YYYY-MM-
    let b = load8(s, 8); // DDTHH:MM
    let c = load8(s, 16); // :SSZ.... or :SS+HH:M
    const A_SEP: u64 = 0xFF00_00FF_0000_0000; // lanes 4, 7
    const A_VAL: u64 = (b'-' as u64) << 32 | (b'-' as u64) << 56;
    const B_SEP: u64 = 0x0000_FF00_00FF_0000; // lanes 2, 5
    const B_VAL: u64 = (b'T' as u64) << 16 | (b':' as u64) << 40;
    if a & A_SEP != A_VAL || b & B_SEP != B_VAL || c & 0xFF != b':' as u64 {
        return None;
    }
    if !all_digits(a, !A_SEP) || !all_digits(b, !B_SEP) || !all_digits(c, 0x00FF_FF00) {
        return None;
    }
    // digit values, separators zeroed; then lane i = 10 * digit i + digit i+1
    let pair = |v: u64, lanes: u64| {
        let t = (v ^ 0x3030_3030_3030_3030) & lanes;
        t.wrapping_mul(10).wrapping_add(t >> 8)
    };
    let pa = pair(a, !A_SEP);
    let pb = pair(b, !B_SEP);
    let pc = pair(c, 0x0000_FFFF_00FF_FF00); // SS, and HH of an offset
    let ln = |v: u64, i: u32| ((v >> (8 * i)) & 0xFF) as u32;
    let y = ln(pa, 0) * 100 + ln(pa, 2);
    let mo = ln(pa, 5);
    let d = ln(pb, 0);
    let h = ln(pb, 3);
    let mi = ln(pb, 6);
    let se = ln(pc, 1);
    let off: i64 = if s.len() == 20 {
        if (c >> 24) & 0xFF != b'Z' as u64 {
            return None;
        }
        0
    } else {
        // c lanes 3..7 = "+HH:M", s[24] = M
        let sign = match (c >> 24) & 0xFF {
            0x2B => 1,
            0x2D => -1,
            _ => return None,
        };
        let m2 = s[24].wrapping_sub(b'0');
        if (c >> 48) & 0xFF != b':' as u64 || !all_digits(c, 0xFF00_FFFF_0000_0000 & !0x00FF_0000_0000_0000) || m2 > 9 {
            return None;
        }
        let oh = ln(pc, 4);
        let om = ln(c ^ 0x3030_3030_3030_3030, 7) * 10 + m2 as u32;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om) as i64
    };
    if mo.wrapping_sub(1) > 11 || h > 23 || mi > 59 || se > 59 || d < 1 {
        return None;
    }
    let (before, leap) = year_info(y);
    const DIM: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if d > DIM[mo as usize] + (leap && mo == 2) as u32 {
        return None;
    }
    let days = days_from_parts(before, leap, mo, d);
    Some(((days * 86_400 + (h * 3600 + mi * 60 + se) as i64) - off * 60) * 1000)
}

/// `ingest_ts`, which the rules guarantee is a valid F1 timestamp: no
/// validation, just the arithmetic. Falls back to the full parser otherwise.
#[inline(always)]
#[allow(dead_code)]
fn parse_ingest(s: &[u8]) -> Option<i64> {
    parse_ingest_s(s).map(|(_, ms)| ms)
}

/// As `parse_ingest`, returning (floor seconds, milliseconds).
#[inline(always)]
fn parse_ingest_s(s: &[u8]) -> Option<(i64, i64)> {
    let slow = |s: &[u8]| parse_ts_fast(s).map(|ms| (ms.div_euclid(1000), ms));
    if s.len() != 20 {
        return slow(s);
    }
    // digit lanes only (separators zeroed so the multiply cannot carry)
    let a = (load8(s, 0) ^ 0x3030_3030_3030_3030) & 0x00FF_FF00_FFFF_FFFF;
    let b = (load8(s, 8) ^ 0x3030_3030_3030_3030) & 0xFFFF_00FF_FF00_FFFF;
    let c = (load8(s, 16) ^ 0x3030_3030_3030_3030) & 0x0000_0000_00FF_FF00;
    let pa = a.wrapping_mul(10).wrapping_add(a >> 8);
    let pb = b.wrapping_mul(10).wrapping_add(b >> 8);
    let pc = c.wrapping_mul(10).wrapping_add(c >> 8);
    let ln = |v: u64, i: u32| ((v >> (8 * i)) & 0xFF) as u32;
    let y = ln(pa, 0) * 100 + ln(pa, 2);
    let (mo, d, h, mi, se) = (ln(pa, 5), ln(pb, 0), ln(pb, 3), ln(pb, 6), ln(pc, 1));
    if mo.wrapping_sub(1) > 11 || d.wrapping_sub(1) > 30 {
        return slow(s);
    }
    let (before, leap) = year_info(y);
    let secs = days_from_parts(before, leap, mo, d) * 86_400 + (h * 3600 + mi * 60 + se) as i64;
    Some((secs, secs * 1000))
}

/// Same contract as `parse_ts`.
#[inline(always)]
fn parse_ts_fast(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_iso_fast(s);
    }
    if n <= 16 {
        // digits only: up to two 8-byte words, left-padded with '0'
        let (hi, lo) = if n > 8 {
            let k = n - 8;
            (load8(s, 0) << (8 * (8 - k)) | (0x3030_3030_3030_3030u64 >> (8 * k)), load8(s, k))
        } else {
            (0x3030_3030_3030_3030, load8(s, 0) << (8 * (8 - n)) | (0x3030_3030_3030_3030u64 >> (8 * n)))
        };
        if !all_digits(hi, !0) || !all_digits(lo, !0) {
            return None;
        }
        return Some((eight_digits(hi) * 100_000_000 + eight_digits(lo)) as i64);
    }
    parse_ts(s)
}

// --- lean timestamp parsing ---------------------------------------------------
// Timestamps are most of the per-line work. The data holds a few dozen
// distinct dates, so "YYYY-MM-DDT" goes through a small validated cache
// (two compares instead of the calendar arithmetic); "HH:MM:SS" is one SWAR
// multiply; digit strings are parsed 16 at a time with SSE.

#[derive(Clone, Copy)]
#[repr(C)]
struct DateEnt {
    a: u64,
    b: u32,
    _pad: u32,
    secs: i64,
}

/// Dates seen by this thread: "YYYY-MM-" and "-DDT" -> seconds at 00:00:00 UTC.
/// Direct-mapped on the day and the parity of the month; only valid dates get in.
struct DateCache([DateEnt; 128]);

impl DateCache {
    fn new() -> DateCache {
        DateCache([DateEnt { a: 0, b: 0, _pad: 0, secs: 0 }; 128])
    }

    /// Seconds at midnight of the date at `s` ("YYYY-MM-DDT", 11 readable
    /// bytes), or None if it is not a valid date followed by 'T'.
    #[inline(always)]
    fn secs(&mut self, s: *const u8) -> Option<i64> {
        let a = unsafe { (s as *const u64).read_unaligned() };
        let b = unsafe { (s.add(7) as *const u32).read_unaligned() };
        let i = ((pext64(b as u64, 0x000F_0300) ^ ((a >> 42) & 0x40)) & 127) as usize;
        let e = unsafe { self.0.get_unchecked(i) };
        if e.a == a && e.b == b {
            return Some(e.secs);
        }
        self.miss(i, a, b)
    }

    #[cold]
    #[inline(never)]
    fn miss(&mut self, i: usize, a: u64, b: u32) -> Option<i64> {
        // a = "YYYY-MM-", b = "-DDT"
        const A_SEP: u64 = 0xFF00_00FF_0000_0000;
        const A_VAL: u64 = (b'-' as u64) << 32 | (b'-' as u64) << 56;
        if a & A_SEP != A_VAL || !all_digits(a, !A_SEP) || b >> 24 != b'T' as u32 || !all_digits(b as u64, 0x00FF_FF00) {
            return None;
        }
        let dg = |v: u64, i: u32| ((v >> (8 * i)) & 0xF) as u32;
        let y = dg(a, 0) * 1000 + dg(a, 1) * 100 + dg(a, 2) * 10 + dg(a, 3);
        let mo = dg(a, 5) * 10 + dg(a, 6);
        let d = dg(b as u64, 1) * 10 + dg(b as u64, 2);
        if mo.wrapping_sub(1) > 11 || d < 1 {
            return None;
        }
        let (before, leap) = year_info(y);
        const DIM: [u32; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        if d > DIM[mo as usize] + (leap && mo == 2) as u32 {
            return None;
        }
        let secs = days_from_parts(before, leap, mo, d) * 86_400;
        self.0[i] = DateEnt { a, b, _pad: 0, secs };
        Some(secs)
    }
}

/// "HH:MM:SS" (the 8 bytes at s) -> (seconds of the day, well-formed and in range).
#[inline(always)]
fn clock(s: *const u8) -> (i64, bool) {
    let w = unsafe { (s as *const u64).read_unaligned() };
    // digits to their values, ':' to 0
    let t = w ^ 0x3030_3A30_303A_3030;
    let digits_ok = ((t.wrapping_add(0x7676_0076_7600_7676) | t) & 0x8080_0080_8000_8080) == 0 && t & 0x0000_FF00_00FF_0000 == 0;
    // lanes 0, 3, 6 = HH, MM, SS
    const L: u64 = 0x00FF_0000_FF00_00FF;
    let p = (t.wrapping_mul(10).wrapping_add(t >> 8)) & L;
    // 23 + 104 = 59 + 68 = 127: a lane over its maximum sets its top bit
    let range_ok = p.wrapping_add(104 | 68 << 24 | 68 << 48) & 0x0080_0000_8000_0080 == 0;
    // lanes are 24 bits apart: one multiply sums 1800 HH + 30 MM into the top
    // 16 bits (the full count of seconds would need 17)
    let half = p.wrapping_mul(1800 << 48 | 30 << 24) >> 48;
    ((2 * half + (p >> 48)) as i64, digits_ok && range_ok)
}

/// 1..=16 ASCII digits -> value, None if any byte is not a digit. 16 bytes
/// from s must be readable.
#[inline(always)]
fn digits16(s: &[u8]) -> Option<u64> {
    #[cfg(target_feature = "avx2")]
    unsafe {
        use std::arch::x86_64::*;
        // CTL[n] right-aligns the first n bytes (zeros to their left)
        static CTL: [[u8; 16]; 17] = {
            let mut t = [[0x80u8; 16]; 17];
            let mut n = 0;
            while n <= 16 {
                let mut i = 0;
                while i < n {
                    t[n][16 - n + i] = i as u8;
                    i += 1;
                }
                n += 1;
            }
            t
        };
        let n = s.len();
        debug_assert!(n >= 1 && n <= 16);
        let x = _mm_loadu_si128(s.as_ptr() as *const __m128i);
        let x = _mm_sub_epi8(x, _mm_set1_epi8(b'0' as i8));
        let x = _mm_shuffle_epi8(x, _mm_loadu_si128(CTL.get_unchecked(n).as_ptr() as *const __m128i));
        let over = _mm_subs_epu8(x, _mm_set1_epi8(9));
        if _mm_testz_si128(over, over) == 0 {
            return None;
        }
        let m = _mm_maddubs_epi16(x, _mm_set1_epi16(0x010A));
        let m = _mm_madd_epi16(m, _mm_set1_epi32(0x0001_0064));
        let m = _mm_packus_epi32(m, m);
        let m = _mm_madd_epi16(m, _mm_set1_epi32(0x0001_2710));
        let r = _mm_cvtsi128_si64(m) as u64;
        Some((r & 0xFFFF_FFFF) * 100_000_000 + (r >> 32))
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let mut v = 0u64;
        for &c in s {
            let d = dig(c);
            if d > 9 {
                return None;
            }
            v = v * 10 + d as u64;
        }
        Some(v)
    }
}

/// Same contract (and results) as `parse_ts`.
#[inline(always)]
fn parse_ts_dc(dc: &mut DateCache, s: &[u8]) -> Option<i64> {
    let n = s.len();
    let p = s.as_ptr();
    if n == 20 {
        // YYYY-MM-DDTHH:MM:SSZ
        if let Some(day) = dc.secs(p) {
            let (sod, ok) = clock(unsafe { p.add(11) });
            if ok && unsafe { *p.add(19) } == b'Z' {
                return Some((day + sod) * 1000);
            }
        }
    } else if n.wrapping_sub(1) < 16 {
        return digits16(s).map(|v| v as i64);
    } else if n == 25 {
        // YYYY-MM-DDTHH:MM:SS+HH:MM
        if let Some(day) = dc.secs(p) {
            let (sod, ok) = clock(unsafe { p.add(11) });
            // "SS+HH:MM": sign in lane 2, HH in lanes 3-4, ':' in 5, MM in 6-7
            let w = unsafe { (p.add(17) as *const u64).read_unaligned() };
            let t = w ^ 0x3030_3A30_3000_0000;
            let digits_ok = ((t.wrapping_add(0x7676_0076_7600_0000) | t) & 0x8080_0080_8000_0000) == 0 && t & 0x0000_FF00_0000_0000 == 0;
            let d = t & 0xFFFF_00FF_FF00_0000;
            let pr = d.wrapping_mul(10).wrapping_add(d >> 8);
            let (oh, om) = ((pr >> 24) & 0xFF, (pr >> 48) & 0xFF);
            let sign = (w >> 16) & 0xFF;
            if ok && digits_ok && oh <= 23 && om <= 59 && (sign == 0x2B || sign == 0x2D) {
                let off = (oh * 60 + om) as i64 * 60;
                return Some((day + sod - if sign == 0x2B { off } else { -off }) * 1000);
            }
        }
    }
    parse_ts_fast(s)
}

/// `ingest_ts` (a valid YYYY-MM-DDTHH:MM:SSZ by the rules) -> (seconds,
/// milliseconds); anything else takes the full parser.
#[inline(always)]
fn parse_ingest_dc(dc: &mut DateCache, s: &[u8]) -> Option<(i64, i64)> {
    if s.len() == 20 {
        if let Some(day) = dc.secs(s.as_ptr()) {
            let secs = day + clock(unsafe { s.as_ptr().add(11) }).0;
            return Some((secs, secs * 1000));
        }
    }
    parse_ts_fast(s).map(|ms| (ms.div_euclid(1000), ms))
}

#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u32 = 0;
    for &c in s {
        let d = dig(c);
        if d > 9 {
            return None;
        }
        v = v * 10 + d;
        if v > 10_000_000 {
            return None;
        }
    }
    if v >= 1 {
        Some(v)
    } else {
        None
    }
}

/// 0 view, 1 cart, 2 purchase, 3 invalid.
#[inline(always)]
fn event_type(s: &[u8]) -> u32 {
    let w = unsafe { (s.as_ptr() as *const u64).read_unaligned() };
    match s.len() {
        4 => {
            let w = w as u32;
            if w == u32::from_le_bytes(*b"view") {
                0
            } else if w == u32::from_le_bytes(*b"cart") {
                1
            } else {
                3
            }
        }
        8 if w == u64::from_le_bytes(*b"purchase") => 2,
        _ => 3,
    }
}

// ---------------------------------------------------------------------------
// Records

const T_PURCHASE: u32 = 2;
/// Dedup record (16 bytes), partitioned by event id: `fp` is a 64-bit event-id
/// fingerprint; `b` = 8 more fingerprint bits << 56 | ingest seconds (24
/// bits, relative to window start - 2^23 s) << 32 | loc. loc points at the
/// payload of a valid copy in the same worker: HAS_PAY | partition << 21 |
/// index.
#[derive(Clone, Copy)]
#[repr(C)]
struct RecD {
    fp: u64,
    b: u64,
}

const HAS_PAY: u64 = 1 << 31;
const IDX_BITS: u32 = 21;
const _: () = assert!(MAX_PC_BITS + IDX_BITS <= 31);
static IDX_OVERFLOW: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

impl RecD {
    #[inline(always)]
    fn ingest(&self) -> u64 {
        (self.b >> 32) & 0xFF_FFFF
    }
    #[inline(always)]
    fn same_event(&self, o: &RecD) -> bool {
        self.fp == o.fp && (self.b >> 56) == (o.b >> 56)
    }
}

/// A key in 12 bytes: exact for strings whose bytes 11..14 are zero (all
/// strings of up to 11 bytes); otherwise an escape (third word 0xFE) holding
/// an index into a side table of full keys.
type Ck = [u32; 3];

const ESCAPE: u32 = 0xFE;

#[inline(always)]
fn compact(k: &Key, esc: &mut PVec<Key>) -> Ck {
    if (k[1] >> 8) & 0xFFFF_FFFF == 0 {
        [(k[0] >> 32) as u32, k[0] as u32, (((k[1] >> 32) as u32) & 0xFFFF_FF00) | (k[1] as u32 & 0xFF)]
    } else {
        let i = esc.len() as u32;
        esc.push(*k);
        [i, 0, ESCAPE]
    }
}

#[inline(always)]
fn expand(c: &Ck, esc: &[Key]) -> Key {
    if c[2] & 0xFF == ESCAPE {
        unsafe { *esc.get_unchecked(c[0] as usize) }
    } else {
        [((c[0] as u64) << 32) | c[1] as u64, (((c[2] & 0xFFFF_FF00) as u64) << 32) | (c[2] & 0xFF) as u64]
    }
}

/// Payload of a valid event copy (16 bytes), partitioned by person: the
/// person and ts | PURCHASE. Purchases' (price, category) go, in the same
/// order, to the partition's side table. A copy that lost the dedup has its
/// ts bits set to TS_DEAD (keeping the PURCHASE bit, so the side table stays
/// aligned).
#[derive(Clone, Copy)]
#[repr(C)]
struct RecP {
    p: Ck,
    t: u32,
}

const PURCHASE: u32 = 1 << 31;
const TS_DEAD: u32 = 0x7FFF_FFFF;

/// A pending payload in the parse batch, before its person is resolved.
#[derive(Clone, Copy)]
struct Pend {
    person: Key,
    ts: u32,
    purchase: bool,
}

/// A click as stored by phase 1 (32 bytes), partitioned by person.
#[derive(Clone, Copy)]
#[repr(C)]
struct RecCc {
    p: Ck,
    c: Ck,
    ts: u32,
    camp: u32,
}

/// A click in phase 3.
#[derive(Clone, Copy)]
#[repr(C)]
struct RecC {
    user: Key, // the person
    cid: Key,
    ts: u32, // ms since window start - 7 days
    camp: u32,
}

#[inline(always)]
fn eid_fp(k: &Key) -> (u64, u32) {
    (hk(k), fm(k[1] ^ 0x5851_F42D_4C95_7F2D, k[0] ^ 0x1405_7B7E_F767_814F) as u32)
}

// ---------------------------------------------------------------------------
// Inputs

struct Ctx {
    ws: i64,
    we: i64,
    ingest_base_s: i64,
    products: KeyMap<u32>,
    unknown_cat: u32,
    cat_names: Vec<Vec<u8>>,
    campaigns: KeyMap<u32>,
    camp_ids: Vec<Vec<u8>>,
    camp_channels: Vec<Vec<u8>>,
}

fn manifest_value(text: &str, name: &str) -> i64 {
    let at = text.find(&format!("\"{name}\"")).expect("manifest key") + name.len() + 2;
    let rest = &text[at..];
    let q1 = rest.find('"').unwrap() + 1;
    let q2 = q1 + rest[q1..].find('"').unwrap();
    let mut v = rest[q1..q2].as_bytes().to_vec();
    v.extend_from_slice(&[0u8; PAD]);
    parse_ts(&v[..q2 - q1]).expect("manifest timestamp")
}

fn csv_files(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut v = Vec::new();
    if let Ok(rd) = fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map(|x| x == "csv").unwrap_or(false) {
                if let Ok(m) = fs::metadata(&p) {
                    if m.is_file() {
                        v.push((p, m.len()));
                    }
                }
            }
        }
    }
    v.sort();
    v
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Link,
    Event,
    Click,
}

struct InFile {
    kind: Kind,
    /// Reopened for the few chunks that cannot be read through the mapping.
    path: PathBuf,
    len: u64,
    /// Read-only mapping of the file followed by at least 4 KB of zeros, or
    /// null when mapping failed.
    map: *const u8,
    ends_nl: bool,
}

unsafe impl Sync for InFile {}

fn map_file(f: &File, len: usize) -> *const u8 {
    use std::os::unix::io::AsRawFd;
    if len == 0 {
        return std::ptr::null();
    }
    let total = ((len + 4095) & !4095) + 8192;
    unsafe {
        let base = mmap(std::ptr::null_mut(), total, 1, 0x02 | 0x20 | 0x4000, -1, 0);
        if base as isize == -1 {
            return std::ptr::null();
        }
        let p = mmap(base, len, 1, 0x02 | 0x10 /* MAP_FIXED */, f.as_raw_fd(), 0);
        if p as isize == -1 || p != base {
            return std::ptr::null();
        }
        p as *const u8
    }
}

struct Chunk {
    kind: Kind,
    file: usize,
    start: u64,
    end: u64,
}

/// Reads the bytes for a chunk into `buf` (ending in '\n', with PAD slack).
/// Returns (first line start, limit) relative to buf: lines starting in
/// [begin, limit) belong to this chunk.
fn read_chunk(f: &File, flen: u64, c: &Chunk, buf: &mut Vec<u8>) -> (usize, usize) {
    let rs = if c.start == 0 { 0 } else { c.start - 1 };
    let mut re = (c.end + 4096).min(flen);
    buf.clear();
    loop {
        let have = buf.len();
        let want = (re - rs) as usize;
        if buf.capacity() < want + PAD + 1 {
            buf.reserve(want + PAD + 1 - have);
        }
        unsafe { buf.set_len(want) };
        let mut off = have;
        while off < want {
            let n = f.read_at(&mut buf[off..want], rs + off as u64).expect("read");
            if n == 0 {
                break;
            }
            off += n;
        }
        buf.truncate(off);
        // Need a '\n' at or after relative (end - 1 - rs), or EOF.
        let need = (c.end - 1 - rs) as usize;
        let done = re >= flen || find_byte(buf, need.min(buf.len()), b'\n') < buf.len();
        if done {
            break;
        }
        re = (re + (1 << 20)).min(flen);
    }
    if buf.last() != Some(&b'\n') {
        buf.push(b'\n');
    }
    let n = buf.len();
    buf.reserve(PAD);
    unsafe {
        std::ptr::write_bytes(buf.as_mut_ptr().add(n), 0, PAD);
    }
    let begin = find_byte(buf, 0, b'\n') + 1;
    (begin, (c.end - rs) as usize)
}

/// The bytes of a chunk: either the file mapping itself or a padded copy.
/// Returns (buffer, begin, limit).
fn chunk_bytes<'a>(inf: &'a InFile, c: &Chunk, buf: &'a mut Vec<u8>) -> (&'a [u8], usize, usize) {
    let all = if inf.map.is_null() { &[][..] } else { unsafe { std::slice::from_raw_parts(inf.map, inf.len as usize) } };
    let ok = !inf.map.is_null()
        && if c.end >= inf.len { inf.ends_nl } else { find_byte(all, c.end as usize - 1, b'\n') < all.len() };
    if ok {
        // populate the page tables for this range in one go
        let s = (c.start as usize) & !4095;
        let e = ((c.end as usize + 65536).min(inf.len as usize) + 4095) & !4095;
        unsafe { madvise(inf.map.add(s) as *mut _, e - s, 22 /* MADV_POPULATE_READ */) };
        let begin = if c.start == 0 {
            find_byte(all, 0, b'\n') + 1
        } else {
            find_byte(all, c.start as usize - 1, b'\n') + 1
        };
        // The buffer handed to the scanner must end with '\n': cut it after
        // the first '\n' at or after end - 1.
        let stop = if c.end >= inf.len { inf.len as usize } else { find_byte(all, c.end as usize - 1, b'\n') + 1 };
        (&all[..stop], begin, c.end as usize)
    } else {
        let file = File::open(&inf.path).expect("open");
        let (b, l) = read_chunk(&file, inf.len, c, buf);
        (&buf[..], b, l)
    }
}

// ---------------------------------------------------------------------------
// Phase 1: parse

static LINKS: Mutex<Vec<Vec<(Key, Key)>>> = Mutex::new(Vec::new());
static LINKMAP: OnceLock<LinkMap> = OnceLock::new();

struct P1Work {
    dd: Parts<RecD, 4>,          // by event-id partition
    pay: Parts<RecP, 4>,         // by person partition
    pur: Vec<PVec<(u32, u32)>>,  // per person partition: (price, category) of purchases, in order
    pesc: Vec<PVec<Key>>,        // per person partition: escaped payload keys
    clq: Parts<RecCc, 2>,        // by person partition
    cesc: Vec<PVec<Key>>,        // per person partition: escaped click keys
}

struct P1Out {
    dd_region: (usize, usize),
    dd: Vec<PVec<RecD>>,
    pay: Vec<PVec<RecP>>,
    pur: Vec<PVec<(u32, u32)>>,
    pesc: Vec<PVec<Key>>,
    clq: Vec<PVec<RecCc>>,
    cesc: Vec<PVec<Key>>,
}

fn parse_links(buf: &[u8], begin: usize, limit: usize) -> Vec<(Key, Key)> {
    // a link line is rarely shorter than 16 bytes
    let mut out = Vec::with_capacity(limit.min(buf.len()).saturating_sub(begin) / 16 + 16);
    scan_lean::<2, 1, _>(buf, begin, limit, #[inline(always)] |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let a = key_of(f[0]);
        let b = key_of(f[1]);
        if a != b {
            out.push((a, b));
        }
    });
    out
}

const EB: usize = 16;

#[derive(Clone, Copy)]
struct EvBatch {
    n: usize,
    pay: [Pend; EB],
    pc: [(u32, u32); EB],
    h: [u64; EB],
    linked: [bool; EB],
    dd: [RecD; EB],
}

/// Pipeline stage 2: the filter words were prefetched while parsing; decide
/// which users need a probe and prefetch their slots.
#[inline(always)]
fn stage2(b: &mut EvBatch, lm: &LinkMap) {
    for j in 0..b.n {
        let linked = lm.maybe_linked(b.h[j]);
        if linked {
            lm.prefetch(b.h[j]);
        }
        b.linked[j] = linked;
    }
}

#[inline(always)]
fn flush_batch(b: &mut EvBatch, lm: &LinkMap, o: &mut P1Work) {
    for j in 0..b.n {
        let mut p = b.pay[j];
        let mut ph = b.h[j];
        if b.linked[j] {
            p.person = lm.person(&p.person, ph);
            ph = hk(&p.person);
        }
        let q = (ph >> (64 - pc_bits())) as usize;
        if p.purchase {
            unsafe { o.pur.get_unchecked_mut(q) }.push(b.pc[j]);
        }
        let rec = RecP { p: compact(&p.person, unsafe { o.pesc.get_unchecked_mut(q) }), t: p.ts | ((p.purchase as u32) << 31) };
        let mut d = b.dd[j];
        let idx = o.pay.next_index(q) as u64;
        if idx >= 1 << IDX_BITS {
            IDX_OVERFLOW.store(true, Ordering::Relaxed);
        }
        d.b |= HAS_PAY | ((q as u64) << IDX_BITS) | (idx & ((1 << IDX_BITS) - 1));
        o.pay.push(q, rec);
        o.dd.push((d.fp >> (64 - pa_bits())) as usize, d);
    }
    b.n = 0;
}

fn parse_events(ctx: &Ctx, buf: &[u8], begin: usize, limit: usize, lm: &LinkMap, o: &mut P1Work) {
    let (ws, we) = (ctx.ws, ctx.we);
    let empty = EvBatch {
        n: 0,
        pay: [Pend { person: ZERO, ts: 0, purchase: false }; EB],
        pc: [(0, 0); EB],
        h: [0; EB],
        linked: [false; EB],
        dd: [RecD { fp: 0, b: 0 }; EB],
    };
    // two batches: `bs[cur]` fills while `bs[cur ^ 1]` waits for its slots
    let mut bs = [empty, empty];
    let mut cur = 0usize;
    let mut dc = DateCache::new();
    scan_lean::<7, 3, _>(buf, begin, limit, #[inline(always)] |f| {
        if f[0].is_empty() {
            return;
        }
        let (fp, fp2) = eid_fp(&key_of(f[0]));
        let (ingest_s, ingest_ms) = parse_ingest_dc(&mut dc, f[3]).unwrap_or((i64::MAX / 2000, i64::MAX / 2));
        let ingest = (ingest_s - ctx.ingest_base_s).clamp(0, 0xFF_FFFF) as u64;
        let d = RecD { fp, b: ((fp2 as u64) << 56) | (ingest << 32) };
        let part = (fp >> (64 - pa_bits())) as usize;
        'v: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'v;
            }
            let ts = match parse_ts_dc(&mut dc, f[2]) {
                Some(t) if t >= ws && t < we => t,
                _ => break 'v,
            };
            if ingest_ms - ts > DAY {
                break 'v;
            }
            let ty = event_type(f[4]);
            let price = match ty {
                0 | 1 => 0,
                2 => match parse_price(f[6]) {
                    Some(p) => p,
                    None => break 'v,
                },
                _ => break 'v,
            };
            let cat = if ty == T_PURCHASE { ctx.products.get(&key_of(f[5])).unwrap_or(ctx.unknown_cat) } else { 0 };
            let user = key_of(f[1]);
            let p = Pend { person: user, ts: (ts - ws) as u32, purchase: ty == T_PURCHASE };
            let h = hk(&user);
            lm.prefetch_filter(h);
            let b = &mut bs[cur];
            b.pay[b.n] = p;
            b.pc[b.n] = (price, cat);
            b.h[b.n] = h;
            b.dd[b.n] = d;
            b.n += 1;
            if b.n == EB {
                stage2(b, lm);
                let [b0, b1] = &mut bs;
                let prev = if cur == 0 { b1 } else { b0 };
                flush_batch(prev, lm, o);
                cur ^= 1;
            }
            return;
        }
        o.dd.push(part, d);
    });
    stage2(&mut bs[cur], lm);
    flush_batch(&mut bs[cur ^ 1], lm, o);
    flush_batch(&mut bs[cur], lm, o);
}

fn parse_clicks(
    ctx: &Ctx,
    buf: &[u8],
    begin: usize,
    limit: usize,
    lm: &LinkMap,
    clq: &mut Parts<RecCc, 2>,
    cesc: &mut [PVec<Key>],
) {
    let lo = ctx.ws - WEEK;
    const B: usize = 16;
    // Two batches, as for events: the filter word is prefetched while parsing,
    // the slot when the batch is full, and the batch is resolved one batch later.
    #[derive(Clone, Copy)]
    struct Batch {
        n: usize,
        r: [RecC; B],
        h: [u64; B],
        linked: [bool; B],
    }
    let empty = Batch { n: 0, r: [RecC { user: ZERO, cid: ZERO, ts: 0, camp: 0 }; B], h: [0; B], linked: [false; B] };
    let mut bs = [empty, empty];
    let mut cur = 0usize;
    let stage2 = |b: &mut Batch| {
        for j in 0..b.n {
            let linked = lm.maybe_linked(b.h[j]);
            if linked {
                lm.prefetch(b.h[j]);
            }
            b.linked[j] = linked;
        }
    };
    let flush = |b: &mut Batch, clq: &mut Parts<RecCc, 2>, cesc: &mut [PVec<Key>]| {
        for j in 0..b.n {
            let mut r = b.r[j];
            let mut ph = b.h[j];
            if b.linked[j] {
                r.user = lm.person(&r.user, ph);
                ph = hk(&r.user);
            }
            let q = (ph >> (64 - pc_bits())) as usize;
            let esc = unsafe { cesc.get_unchecked_mut(q) };
            let rec = RecCc { p: compact(&r.user, esc), c: compact(&r.cid, esc), ts: r.ts, camp: r.camp };
            clq.push(q, rec);
        }
        b.n = 0;
    };
    let mut dc = DateCache::new();
    scan_lean::<4, 2, _>(buf, begin, limit, #[inline(always)] |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let ts = match parse_ts_dc(&mut dc, f[3]) {
            Some(t) if t >= lo && t < ctx.we => t,
            _ => return,
        };
        let camp = match ctx.campaigns.get(&key_of(f[2])) {
            Some(c) => c,
            None => return,
        };
        let user = key_of(f[1]);
        let h = hk(&user);
        lm.prefetch_filter(h);
        let b = &mut bs[cur];
        b.r[b.n] = RecC { user, cid: key_of(f[0]), ts: (ts - lo) as u32, camp };
        b.h[b.n] = h;
        b.n += 1;
        if b.n == B {
            stage2(b);
            let [b0, b1] = &mut bs;
            let prev = if cur == 0 { b1 } else { b0 };
            flush(prev, clq, cesc);
            cur ^= 1;
        }
    });
    stage2(&mut bs[cur]);
    flush(&mut bs[cur ^ 1], clq, cesc);
    flush(&mut bs[cur], clq, cesc);
}

// ---------------------------------------------------------------------------
// Phase 3: sessions

struct P3 {
    uw: pq::UserWriter,
    name: Vec<u8>,
    agg: Vec<[u64; 4]>, // (day * (ncamp + 1) + camp) -> sessions, purchasing, revenue, assisted
    assist: Vec<u32>,   // (day * (ncamp + 1) + camp) -> assisted sessions
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    pend: Vec<u32>,
    pend_days: Vec<u32>,
    catsum: Vec<u64>,
    touched: Vec<u32>,
    /// One person's events by 30-minute bucket: [min ts, max ts, count, 0]
    /// (empty: [u32::MAX, 0, 0, 0]), and which buckets are in use.
    cell: Vec<[u32; 4]>,
    bits: Vec<u64>,
    /// The person's purchases (ts << 32 | x).
    purs: Vec<u64>,
}

struct Geo {
    ws: i64,
    day0: i64,
    organic: u32,
}

/// `evs`: the person's events as ts << 32 | x, in any order; `pur`: (price,
/// cat) for the purchase indices in x.
///
/// Events are not sorted: two events less than 30 minutes apart are always in
/// one session, so events are summarized per 30-minute bucket (min, max,
/// count) and sessions are built from the buckets in time order: a session
/// breaks between two used buckets iff min(next) - max(previous) > 30 min.
/// `cls`: the person's clicks as ts << 32 | index into `camps`, sorted by ts,
/// ties by click id descending (so the last of equal ts has the smallest id).
fn process_person(g: &Geo, st: &mut P3, person: Key, evs: &[u64], pur: &[(u32, u32)], cls: &[u64], camps: &[u32]) {
    let mut lo = 0usize;
    let mut hi = 0usize;
    // day accumulators
    let mut cur_day: i64 = i64::MIN;
    let mut d_sessions = 0u32;
    let mut d_events = 0u32;
    let mut d_rev = 0u64;
    // session accumulators
    let mut s_start: i64 = -1;
    let mut s_didx = 0usize;
    let mut s_rev = 0u64;
    let mut s_purch = false;
    let mut prev_t: i64 = 0;

    let stride = g.organic as usize + 1;
    // Sessions not yet credited to the assisting campaigns, per day: they are
    // added to every active campaign when the active set changes.
    macro_rules! flush_assists {
        () => {{
            for &d in &st.pend_days {
                let n = st.pend[d as usize];
                st.pend[d as usize] = 0;
                let row = d as usize * stride;
                for &c in &st.active {
                    unsafe { *st.assist.get_unchecked_mut(row + c as usize) += n };
                }
            }
            st.pend_days.clear();
        }};
    }
    macro_rules! finish_session {
        () => {{
            let sc = s_start + WEEK;
            while hi < cls.len() && ((cls[hi] >> 32) as i64) <= sc {
                let c = unsafe { *camps.get_unchecked(cls[hi] as u32 as usize) } as usize;
                if st.cnt[c] == 0 {
                    flush_assists!();
                    st.pos[c] = st.active.len() as u32;
                    st.active.push(c as u32);
                }
                st.cnt[c] += 1;
                hi += 1;
            }
            while lo < hi && ((cls[lo] >> 32) as i64) < sc - WEEK {
                let c = unsafe { *camps.get_unchecked(cls[lo] as u32 as usize) } as usize;
                st.cnt[c] -= 1;
                if st.cnt[c] == 0 {
                    flush_assists!();
                    let p = st.pos[c] as usize;
                    let last = st.active.pop().unwrap();
                    if p < st.active.len() {
                        st.active[p] = last;
                        st.pos[last as usize] = p as u32;
                    }
                }
                lo += 1;
            }
            let credit = if lo < hi { unsafe { *camps.get_unchecked(cls[hi - 1] as u32 as usize) } } else { g.organic } as usize;
            let a = unsafe { st.agg.get_unchecked_mut(s_didx * (g.organic as usize + 1) + credit) };
            a[0] += 1;
            a[1] += s_purch as u64;
            a[2] += s_rev;
            if !st.active.is_empty() {
                if st.pend[s_didx] == 0 {
                    st.pend_days.push(s_didx as u32);
                }
                st.pend[s_didx] += 1;
            }
        }};
    }
    macro_rules! flush_day {
        () => {{
            let mut top = -1i32;
            let mut best = 0u64;
            for &c in &st.touched {
                let v = st.catsum[c as usize];
                if v > best || (v == best && (c as i32) < top) {
                    best = v;
                    top = c as i32;
                }
                st.catsum[c as usize] = 0;
            }
            st.touched.clear();
            st.uw.row((cur_day - g.day0) as usize, d_sessions, d_events, d_rev, top);
        }};
    }

    for &e in evs {
        let t = (e >> 32) as u32;
        let b = (t / GAP as u32) as usize;
        unsafe {
            *st.bits.get_unchecked_mut(b >> 6) |= 1 << (b & 63);
            let c = st.cell.get_unchecked_mut(b);
            c[0] = c[0].min(t);
            c[1] = c[1].max(t);
            c[2] += 1;
        }
        if e as u32 & PURCHASE != 0 {
            st.purs.push(e);
        }
    }
    if st.purs.len() > 1 {
        st.purs.sort_unstable();
    }
    let mut pi = 0usize;
    for wi in 0..st.bits.len() {
        let mut word = st.bits[wi];
        if word == 0 {
            continue;
        }
        st.bits[wi] = 0;
        while word != 0 {
            let b = wi * 64 + word.trailing_zeros() as usize;
            word &= word - 1;
            let c = std::mem::replace(unsafe { st.cell.get_unchecked_mut(b) }, [u32::MAX, 0, 0, 0]);
            let (t, last) = (c[0] as i64, c[1] as i64);
            if s_start < 0 || t - prev_t > GAP {
                if s_start >= 0 {
                    finish_session!();
                }
                s_start = t;
                s_rev = 0;
                s_purch = false;
                let day = (g.ws + t).div_euclid(DAY);
                s_didx = (day - g.day0) as usize;
                if day != cur_day {
                    if cur_day != i64::MIN {
                        flush_day!();
                        d_sessions = 0;
                        d_events = 0;
                        d_rev = 0;
                    }
                    cur_day = day;
                }
                d_sessions += 1;
            }
            prev_t = last;
            d_events += c[2];
            // the purchases of this bucket
            while pi < st.purs.len() && (st.purs[pi] >> 32) as i64 <= last {
                let e = st.purs[pi];
                pi += 1;
                let (p, c) = unsafe { *pur.get_unchecked((e as u32 & !PURCHASE) as usize) };
                let p = p as u64;
                s_rev += p;
                s_purch = true;
                d_rev += p;
                let c = c as usize;
                if st.catsum[c] == 0 {
                    st.touched.push(c as u32);
                }
                st.catsum[c] += p;
            }
        }
    }
    st.purs.clear();
    if s_start >= 0 {
        finish_session!();
        flush_day!();
        st.name.clear();
        key_bytes(&person, &mut st.name);
        st.uw.end_person(&st.name);
    }
    flush_assists!();
    for &c in &st.active {
        st.cnt[c as usize] = 0;
    }
    st.active.clear();
}

/// Per-partition local person table (generation-stamped, never cleared).
struct LocalIds {
    slots: Vec<LSlot>,
    gen: u32,
    mask: usize,
    persons: Vec<Key>,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct LSlot {
    k: Key,
    id: u32,
    stamp: u32,
}

impl LocalIds {
    fn new() -> LocalIds {
        LocalIds { slots: Vec::new(), gen: 0, mask: 0, persons: Vec::new() }
    }
    /// Starts a new partition expecting about `n` persons.
    fn reset(&mut self, n: usize) {
        let size = (n * 2).next_power_of_two().max(256);
        if size > self.slots.len() {
            self.slots = vec![LSlot { k: ZERO, id: 0, stamp: 0 }; size];
            self.gen = 0;
        }
        self.mask = size - 1;
        self.gen += 1;
        self.persons.clear();
    }
    #[cold]
    fn grow(&mut self) {
        let size = (self.mask + 1) * 2;
        if size > self.slots.len() {
            self.slots = vec![LSlot { k: ZERO, id: 0, stamp: 0 }; size];
            self.gen = 0;
        }
        self.mask = size - 1;
        self.gen += 1;
        for (id, k) in self.persons.iter().enumerate() {
            let mut i = (hk(k) >> 20) as usize & self.mask;
            while self.slots[i].stamp == self.gen {
                i = (i + 1) & self.mask;
            }
            self.slots[i] = LSlot { k: *k, id: id as u32, stamp: self.gen };
        }
    }
    #[inline(always)]
    fn intern(&mut self, k: &Key) -> u32 {
        let mut i = (hk(k) >> 20) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked_mut(i) };
            if s.stamp != self.gen {
                let id = self.persons.len() as u32;
                *s = LSlot { k: *k, id, stamp: self.gen };
                self.persons.push(*k);
                if self.persons.len() * 2 > self.mask + 1 {
                    self.grow();
                }
                return id;
            }
            if s.k == *k {
                return s.id;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: &Key) -> Option<u32> {
        let mut i = (hk(k) >> 20) as usize & self.mask;
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.stamp != self.gen {
                return None;
            }
            if s.k == *k {
                return Some(s.id);
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------------------------------------------------------------------------

fn env_or(name: &str, dflt: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| dflt.to_string())
}

/// Hands out work-item indices to threads.
fn run_workers<R: Send, F: Fn(usize) -> R + Sync>(n: usize, f: F) -> Vec<R> {
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..n)
            .map(|w| {
                let f = &f;
                s.spawn(move || {
                    SUB.with(|c| c.set(w));
                    f(w)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

fn main() {
    let t0 = Instant::now();
    raise_fd_limit();
    let data = PathBuf::from(env_or("DATA_DIR", "/data"));
    let out = PathBuf::from(env_or("OUTPUT_DIR", "/output"));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .clamp(1, 31);
    SUBS.store(nthreads + 1, Ordering::Relaxed);
    let verbose = cfg!(feature = "diag") || std::env::var("SS_VERBOSE").is_ok();
    #[cfg(feature = "diag")]
    if std::env::var_os("SS_CHILD").is_none() {
        diag::run(&data, &out, nthreads);
    }
    #[cfg(feature = "diag")]
    let t0 = Instant::now();
    let lap = |what: &str| {
        if verbose {
            let mut ru = [0i64; 18];
            unsafe { getrusage(0, &mut ru) };
            eprintln!(
                "{:>8.1} ms  {what}  (user {:.2}s sys {:.2}s minflt {})",
                t0.elapsed().as_secs_f64() * 1e3,
                ru[0] as f64 + ru[1] as f64 * 1e-6,
                ru[2] as f64 + ru[3] as f64 * 1e-6,
                ru[8]
            );
        }
    };

    let prof_path = std::env::var("SS_PROF").ok();
    if prof_path.is_some() {
        prof::start(200);
    }
    // --- small inputs
    let manifest = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_value(&manifest, "window_start");
    let we = manifest_value(&manifest, "window_end");
    // event times are stored as 31-bit offsets from the window start
    assert!(we - ws < TS_DEAD as i64, "window longer than 24 days");

    let build_ctx = || {
        let pbuf = pad_buffer(fs::read(data.join("products.csv")).unwrap_or_default());
        let mut prod_raw: Vec<(Key, u32)> = Vec::new();
        let mut cat_ids: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
        let mut cat_list: Vec<Vec<u8>> = Vec::new();
        {
            let begin = find_byte(&pbuf, 0, b'\n') + 1;
            scan_lines::<2, _>(&pbuf, begin, pbuf.len(), |f| {
                if f[0].is_empty() {
                    return;
                }
                let c = match cat_ids.get(f[1]) {
                    Some(&c) => c,
                    None => {
                        let c = cat_list.len() as u32;
                        cat_ids.insert(f[1].to_vec(), c);
                        cat_list.push(f[1].to_vec());
                        c
                    }
                };
                prod_raw.push((key_of(f[0]), c));
            });
        }
        let mut cat_names: Vec<Vec<u8>> = cat_list.clone();
        cat_names.push(b"unknown".to_vec());
        cat_names.sort();
        cat_names.dedup();
        let remap: Vec<u32> = cat_list.iter().map(|c| cat_names.binary_search(c).unwrap() as u32).collect();
        let unknown_cat = cat_names.binary_search(&b"unknown".to_vec()).unwrap() as u32;
        let mut products: KeyMap<u32> = KeyMap::new(prod_raw.len() + 16);
        for (k, c) in &prod_raw {
            products.get_or_insert(k, remap[*c as usize]);
        }

        let cbuf = pad_buffer(fs::read(data.join("campaigns.csv")).unwrap_or_default());
        let mut camp_ids = Vec::new();
        let mut camp_channels = Vec::new();
        let mut campaigns: KeyMap<u32> = KeyMap::new(cbuf.len() / 8 + 16);
        {
            let begin = find_byte(&cbuf, 0, b'\n') + 1;
            scan_lines::<2, _>(&cbuf, begin, cbuf.len(), |f| {
                if f[0].is_empty() {
                    return;
                }
                let (_, ins) = campaigns.get_or_insert(&key_of(f[0]), camp_ids.len() as u32);
                if ins {
                    camp_ids.push(f[0].to_vec());
                    camp_channels.push(f[1].to_vec());
                }
            });
        }
        Ctx {
            ws,
            we,
            ingest_base_s: ws.div_euclid(1000) - (1 << 23),
            products,
            unknown_cat,
            cat_names,
            campaigns,
            camp_ids,
            camp_channels,
        }
    };
    let ctx_cell: OnceLock<Ctx> = OnceLock::new();
    lap("small inputs");

    // --- files and chunks: links first, then clicks (they do not need the
    // link map), then events.
    let use_mmap = std::env::var_os("SS_NO_MMAP").is_none();
    let mut files: Vec<InFile> = Vec::new();
    for (kind, dir) in [(Kind::Link, "links"), (Kind::Click, "clicks"), (Kind::Event, "events")] {
        for (p, len) in csv_files(&data.join(dir)) {
            let file = File::open(&p).expect("open");
            let map = if use_mmap { map_file(&file, len as usize) } else { std::ptr::null() };
            let ends_nl = !map.is_null() && unsafe { *map.add(len as usize - 1) } == b'\n';
            // the mapping outlives the descriptor
            drop(file);
            files.push(InFile { kind, path: p, len, map, ends_nl });
        }
    }
    let mut chunks: Vec<Chunk> = Vec::new();
    for (fi, f) in files.iter().enumerate() {
        let mut s = 0;
        while s < f.len {
            // link files are few and small: small chunks keep every thread busy
            let step = if f.kind == Kind::Link { 256 << 10 } else { chunk_size() };
            let e = (s + step).min(f.len);
            chunks.push(Chunk { kind: f.kind, file: fi, start: s, end: e });
            s = e;
        }
    }
    // estimate line counts to pre-size the partition buffers
    let bytes_of = |k: Kind| files.iter().filter(|f| f.kind == k).map(|f| f.len).sum::<u64>();
    let est_events = (bytes_of(Kind::Event) / 70) as usize;
    let est_clicks = (bytes_of(Kind::Click) / 40) as usize;
    let per = |n: usize, parts: usize| n / parts / nthreads + n / parts / nthreads / 3 + 64;
    // about 40k events per partition, between 256 and 1024 partitions
    let bits = ((est_events / 40_000).max(1) as f64).log2().round().clamp(8.0, MAX_PC_BITS as f64) as u32;
    let abits = std::env::var("SS_ABITS").ok().and_then(|v| v.parse().ok()).unwrap_or(bits);
    let bits = std::env::var("SS_CBITS").ok().and_then(|v| v.parse().ok()).unwrap_or(bits);
    PA_BITS_V.store(abits, Ordering::Relaxed);
    PC_BITS_V.store(bits, Ordering::Relaxed);
    let (pa, pc) = (1usize << abits, 1usize << bits);
    if verbose {
        eprintln!("  partitions: {pa} x {pc} (estimated {est_events} events)");
    }
    let (res_dd, res_pay, res_cl) = (per(est_events, pa), per(est_events, pc), per(est_clicks, pc));
    lap("chunks");

    // --- phase 1: links (all threads build the link map together), then
    // clicks and events.
    let link_chunks: Vec<usize> = (0..chunks.len()).filter(|&i| chunks[i].kind == Kind::Link).collect();
    let data_chunks: Vec<usize> = (0..chunks.len()).filter(|&i| chunks[i].kind != Kind::Link).collect();
    let barrier = std::sync::Barrier::new(nthreads);
    let total_pairs = AtomicUsize::new(0);
    let all_pairs: OnceLock<Vec<Vec<(Key, Key)>>> = OnceLock::new();
    let counters: [AtomicUsize; 6] = Default::default();
    // products and campaigns are read by a thread of their own, so the link
    // phases never wait for them; the workers need them only after the link map
    let p1: Vec<P1Out> = std::thread::scope(|sc| {
        sc.spawn(|| {
            ctx_cell.get_or_init(&build_ctx);
        });
        run_workers(nthreads, |w| {
        let mut buf = Vec::new();
        // L0: parse link files
        loop {
            let i = counters[0].fetch_add(1, Ordering::Relaxed);
            if i >= link_chunks.len() {
                break;
            }
            let c = &chunks[link_chunks[i]];
            let (bytes, b, l) = chunk_bytes(&files[c.file], c, &mut buf);
            let pairs = parse_links(bytes, b, l);
            total_pairs.fetch_add(pairs.len(), Ordering::Relaxed);
            LINKS.lock().unwrap().push(pairs);
        }
        barrier.wait();
        if w == 0 && verbose {
            eprintln!("{:>8.1} ms  links parsed", t0.elapsed().as_secs_f64() * 1e3);
        }
        let lm = LINKMAP.get_or_init(|| LinkMap::with_pairs(total_pairs.load(Ordering::Relaxed)));
        let pairs = all_pairs.get_or_init(|| std::mem::take(&mut *LINKS.lock().unwrap()));
        // Fault the link arrays in, in parallel and in order.
        {
            const PS2: usize = 1 << 16;
            let n = lm.size().div_ceil(PS2);
            let touch = |p: *const u8, len: usize| {
                let mut o = 0;
                while o < len {
                    unsafe { std::ptr::write_volatile(p.add(o) as *mut u8, 0) };
                    o += 4096;
                }
            };
            loop {
                let i = counters[5].fetch_add(1, Ordering::Relaxed);
                if i >= n {
                    break;
                }
                let (a, b) = (i * PS2, ((i + 1) * PS2).min(lm.size()));
                touch(unsafe { lm.slots.add(a) } as *const u8, (b - a) * std::mem::size_of::<Slot>());
                touch(lm.parent[a..b].as_ptr() as *const u8, (b - a) * 4);
            }
            barrier.wait();
            if w == 0 && verbose {
                eprintln!("{:>8.1} ms  link arrays touched", t0.elapsed().as_secs_f64() * 1e3);
            }
        }
        // L1: insert keys and union, in slices
        const PS: usize = 8192;
        let items: Vec<(usize, usize)> =
            pairs.iter().enumerate().flat_map(|(li, v)| (0..v.len()).step_by(PS).map(move |a| (li, a))).collect();
        loop {
            let i = counters[1].fetch_add(1, Ordering::Relaxed);
            if i >= items.len() {
                break;
            }
            let (li, a) = items[i];
            let v = &pairs[li];
            lm.add_pairs(&v[a..(a + PS).min(v.len())]);
        }
        barrier.wait();
        if w == 0 && verbose {
            eprintln!("{:>8.1} ms  links inserted", t0.elapsed().as_secs_f64() * 1e3);
        }
        // L2: smallest key per person; L3: store names
        const SS: usize = 1 << 16;
        let nslices = lm.size().div_ceil(SS);
        loop {
            let i = counters[3].fetch_add(1, Ordering::Relaxed);
            if i >= nslices {
                break;
            }
            lm.finish(i * SS, ((i + 1) * SS).min(lm.size()));
        }
        barrier.wait();
        if w == 0 && verbose {
            eprintln!("{:>8.1} ms  link map built ({} pairs)", t0.elapsed().as_secs_f64() * 1e3, total_pairs.load(Ordering::Relaxed));
        }
        let ctx = ctx_cell.get_or_init(&build_ctx);
        let mut o = P1Work {
            dd: Parts::new(pa, res_dd),
            pay: Parts::new(pc, res_pay),
            pur: (0..pc).map(|_| PVec::with_capacity(res_pay / 24 + 16)).collect(),
            pesc: (0..pc).map(|_| PVec::with_capacity(16)).collect(),
            clq: Parts::new(pc, res_cl),
            cesc: (0..pc).map(|_| PVec::with_capacity(16)).collect(),
        };
        // the dedup buffers were allocated first, contiguously; they are dead
        // after phase 2a and phase 3 reuses that memory
        let dd_region = {
            let first = o.dd.out[0].ptr as usize;
            let last = o.dd.out[pa - 1].ptr as usize + o.dd.out[pa - 1].cap * std::mem::size_of::<RecD>();
            (first, last)
        };

        loop {
            let i = counters[4].fetch_add(1, Ordering::Relaxed);
            if i >= data_chunks.len() {
                break;
            }
            let c = &chunks[data_chunks[i]];
            let (bytes, b, l) = chunk_bytes(&files[c.file], c, &mut buf);
            match c.kind {
                Kind::Event => parse_events(ctx, bytes, b, l, lm, &mut o),
                Kind::Click => parse_clicks(ctx, bytes, b, l, lm, &mut o.clq, &mut o.cesc),
                Kind::Link => unreachable!(),
            }
            let inf = &files[c.file];
            if !inf.map.is_null() {
                release(unsafe { inf.map.add(c.start as usize) }, (c.end - c.start) as usize);
            }
        }
        let r = P1Out {
            dd_region,
            dd: o.dd.finish(),
            pay: o.pay.finish(),
            pur: o.pur,
            pesc: o.pesc,
            clq: o.clq.finish(),
            cesc: o.cesc,
        };
        if verbose {
            eprintln!("  p1 w{w} done at {:.1} ms", t0.elapsed().as_secs_f64() * 1e3);
        }
        r
        })
    });
    lap("phase 1 parse");
    let ctx = ctx_cell.get().unwrap();
    let ncamp = ctx.camp_ids.len();
    let linkmap = LINKMAP.get().unwrap();
    assert!(!IDX_OVERFLOW.load(Ordering::Relaxed), "a person partition outgrew the dedup index");

    // --- phase 2a: dedup by event id; losing copies are marked dead in place
    let next = AtomicUsize::new(0);
    let kill = |w: usize, b: u64| {
        if b & HAS_PAY == 0 {
            return;
        }
        let q = ((b >> IDX_BITS) as usize) & (pc - 1);
        let i = (b as usize) & ((1 << IDX_BITS) - 1);
        unsafe {
            let r = &mut *p1[w].pay[q].ptr.add(i);
            r.t = (r.t & PURCHASE) | TS_DEAD;
        }
    };
    for v in all_pairs.get().unwrap() {
        release_vec(v);
    }
    let lm_slices = linkmap.size().div_ceil(1 << 16);
    let next_lm = AtomicUsize::new(0);
    run_workers(nthreads, |_| {
        let mut table: Vec<u64> = Vec::new();
        let mut seen1: Vec<u64> = Vec::new();
        let mut seen2: Vec<u64> = Vec::new();
        let mut cand: Vec<u64> = Vec::new();
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= pa {
                let j = next_lm.fetch_add(1, Ordering::Relaxed);
                if j < lm_slices {
                    let n = (1usize << 16).min(linkmap.size() - (j << 16));
                    release(unsafe { linkmap.slots.add(j << 16) } as *const u8, n * std::mem::size_of::<Slot>());
                    continue;
                }
                break;
            }
            let total: usize = p1.iter().map(|o| o.dd[i].len()).sum();
            if total == 0 {
                continue;
            }
            // Pass 1: two bitmaps ("seen", "seen twice") over fingerprint bits;
            // nearly every event id is unique, so only the copies whose bit was
            // seen twice (true duplicates plus a few collisions) need the exact
            // table in pass 2.
            let m = (total * 16).next_power_of_two().max(64);
            let pmask = m - 1;
            seen1.clear();
            seen1.resize(m / 64, 0);
            seen2.clear();
            seen2.resize(m / 64, 0);
            for o in p1.iter() {
                for r in o.dd[i].as_slice() {
                    let pos = (r.fp >> 24) as usize & pmask;
                    let (wi, bit) = (pos >> 6, 1u64 << (pos & 63));
                    unsafe {
                        let s1 = *seen1.get_unchecked(wi);
                        if s1 & bit != 0 {
                            *seen2.get_unchecked_mut(wi) |= bit;
                        } else {
                            *seen1.get_unchecked_mut(wi) = s1 | bit;
                        }
                    }
                }
            }
            cand.clear();
            for (w, o) in p1.iter().enumerate() {
                for (j, r) in o.dd[i].as_slice().iter().enumerate() {
                    let pos = (r.fp >> 24) as usize & pmask;
                    if unsafe { *seen2.get_unchecked(pos >> 6) } >> (pos & 63) & 1 != 0 {
                        cand.push(((w as u64) << 32) | j as u64);
                    }
                }
            }
            if cand.len() < 2 {
                continue;
            }
            let size = (cand.len() * 2).next_power_of_two().max(16);
            let mask = size - 1;
            table.clear();
            table.resize(size, 0);
            // entry: tag (24 bits of fp) << 40 | worker << 32 | index, never 0
            for &c in &cand {
                let (w, j) = ((c >> 32) as usize, (c & 0xFFFF_FFFF) as usize);
                let r = unsafe { p1.get_unchecked(w).dd.get_unchecked(i).as_slice().get_unchecked(j) };
                let tag = (r.fp & 0xFF_FFFF) | 0x80_0000;
                let entry = (tag << 40) | c;
                let mut sl = (r.fp >> 24) as usize & mask;
                loop {
                    let e = unsafe { *table.get_unchecked(sl) };
                    if e == 0 {
                        unsafe { *table.get_unchecked_mut(sl) = entry };
                        break;
                    }
                    if e >> 40 == tag {
                        let ow = ((e >> 32) & 0xFF) as usize;
                        let oj = (e & 0xFFFF_FFFF) as usize;
                        let other = unsafe { p1.get_unchecked(ow).dd.get_unchecked(i).as_slice().get_unchecked(oj) };
                        if other.same_event(r) {
                            if r.ingest() < other.ingest() {
                                kill(ow, other.b);
                                unsafe { *table.get_unchecked_mut(sl) = entry };
                            } else {
                                kill(w, r.b);
                            }
                            break;
                        }
                    }
                    sl = (sl + 1) & mask;
                }
            }
        }
        release_vec(&table);
    });
    lap("phase 2a dedup");
    for (w, o) in p1.iter().enumerate() {
        recycle(w, o.dd_region.0, o.dd_region.1);
    }


    // --- phase 3: sessionize per person partition
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let geo = Geo { ws, day0, organic: ncamp as u32 };
    // 30-minute buckets over the window (event times are 31-bit offsets from its start)
    let nbuckets = ((((we - ws).clamp(0, 1 << 31) / GAP) as usize + 2) + 63) / 64 * 64;
    let ncat = ctx.cat_names.len();
    let _ = fs::create_dir_all(out.join("user_daily"));
    let _ = fs::create_dir_all(out.join("campaign_daily"));
    // biggest partitions first for load balance
    let mut order: Vec<(usize, usize)> = (0..pc)
        .map(|q| (p1.iter().map(|o| o.pay[q].len()).sum::<usize>(), q))
        .collect();
    order.sort_unstable_by(|a, b| b.cmp(a));
    let max_part = order[0].0;
    let max_clicks = (0..pc).map(|q| p1.iter().map(|o| o.clq[q].len()).sum::<usize>()).max().unwrap_or(0);
    let next = AtomicUsize::new(0);
    let end_barrier = std::sync::Barrier::new(nthreads);
    let aggs: Vec<Vec<[u64; 4]>> = run_workers(nthreads, |w| {
        let mut st = P3 {
            uw: pq::UserWriter::new(&out.join("user_daily"), w, day0 as i32, ndays, &ctx.cat_names),
            name: Vec::new(),
            agg: vec![[0u64; 4]; (ncamp + 1) * ndays],
            assist: vec![0; (ncamp + 1) * ndays],
            cnt: vec![0; ncamp + 1],
            pos: vec![0; ncamp + 1],
            active: Vec::new(),
            pend: vec![0; ndays],
            pend_days: Vec::new(),
            catsum: vec![0; ncat],
            touched: Vec::new(),
            cell: vec![[u32::MAX, 0, 0, 0]; nbuckets],
            bits: vec![0; nbuckets / 64],
            purs: Vec::new(),
        };
        let mut lid = LocalIds::new();
        lid.reset(max_part / 16);
        // fixed-capacity scratch from this thread's (recycled) arena: never
        // grows past its capacity, never dropped
        let mut evs = arena_vec::<u64>(max_part);
        let mut sorted = arena_vec::<u64>(max_part);
        let mut pids = arena_vec::<u32>(max_part);
        let mut lpur = arena_vec::<(u32, u32)>(max_part);
        let mut cls = arena_vec::<u64>(max_clicks);
        let mut csorted = arena_vec::<u64>(max_clicks);
        let mut ccamp = arena_vec::<u32>(max_clicks);
        let mut ccid = arena_vec::<Key>(max_clicks);
        let mut cpid = arena_vec::<u32>(max_clicks);
        let mut start: Vec<u32> = Vec::new();
        let mut cstart: Vec<u32> = Vec::new();
        let mut fill: Vec<u32> = Vec::new();
        let mut tm = [0f64; 7];
        let mut tt = Instant::now();
        macro_rules! tick {
            ($i:expr) => {
                if verbose {
                    let n = Instant::now();
                    tm[$i] += (n - tt).as_secs_f64();
                    tt = n;
                }
            };
        }
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= pc {
                break;
            }
            let (ne, q) = order[i];
            if ne == 0 {
                continue;
            }
            // gather live events: local person id + packed value
            lid.reset(ne / 16);
            evs.clear();
            pids.clear();
            lpur.clear();
            for o in p1.iter() {
                let side = o.pur[q].as_slice();
                let esc = o.pesc[q].as_slice();
                let mut pk = 0usize;
                for e in o.pay[q].as_slice() {
                    let purchase = e.t >> 31;
                    let ts = e.t & TS_DEAD;
                    if ts == TS_DEAD {
                        pk += purchase as usize;
                        continue;
                    }
                    pids.push(lid.intern(&expand(&e.p, esc)));
                    let x = if purchase != 0 {
                        lpur.push(unsafe { *side.get_unchecked(pk) });
                        pk += 1;
                        PURCHASE | (lpur.len() - 1) as u32
                    } else {
                        0
                    };
                    evs.push(((ts as u64) << 32) | x as u64);
                }
            }
            if evs.is_empty() {
                continue;
            }
            tick!(0);
            let np = lid.persons.len();
            // counting sort by pid
            start.clear();
            start.resize(np + 1, 0);
            for &p in pids.iter() {
                start[p as usize + 1] += 1;
            }
            for k in 0..np {
                start[k + 1] += start[k];
            }
            sorted.clear();
            sorted.reserve(evs.len());
            unsafe { sorted.set_len(evs.len()) };
            fill.clear();
            fill.extend_from_slice(&start[..np]);
            for (&e, &p) in evs.iter().zip(pids.iter()) {
                let f = unsafe { fill.get_unchecked_mut(p as usize) };
                unsafe { *sorted.get_unchecked_mut(*f as usize) = e };
                *f += 1;
            }
            tick!(1);
            // clicks of persons that have events here
            cls.clear();
            cpid.clear();
            ccamp.clear();
            ccid.clear();
            for o in p1.iter() {
                let esc = o.cesc[q].as_slice();
                for c in o.clq[q].as_slice() {
                    let user = expand(&c.p, esc);
                    if let Some(id) = lid.get(&user) {
                        cls.push(((c.ts as u64) << 32) | ccamp.len() as u64);
                        ccamp.push(c.camp);
                        ccid.push(expand(&c.c, esc));
                        cpid.push(id);
                    }
                }
            }
            cstart.clear();
            cstart.resize(np + 1, 0);
            for &p in cpid.iter() {
                cstart[p as usize + 1] += 1;
            }
            for k in 0..np {
                cstart[k + 1] += cstart[k];
            }
            csorted.clear();
            csorted.reserve(cls.len());
            unsafe { csorted.set_len(cls.len()) };
            fill.clear();
            fill.extend_from_slice(&cstart[..np]);
            for (c, &p) in cls.iter().zip(cpid.iter()) {
                let f = &mut fill[p as usize];
                csorted[*f as usize] = *c;
                *f += 1;
            }
            tick!(2);
            tick!(5);
            for p in 0..np {
                let cl = &mut csorted[cstart[p] as usize..cstart[p + 1] as usize];
                if cl.len() > 1 {
                    cl.sort_unstable();
                    // equal timestamps: order by click id, descending
                    let mut a = 0;
                    while a < cl.len() {
                        let mut b = a + 1;
                        while b < cl.len() && cl[b] >> 32 == cl[a] >> 32 {
                            b += 1;
                        }
                        if b - a > 1 {
                            cl[a..b].sort_unstable_by(|x, y| cmp_key(&ccid[*y as u32 as usize], &ccid[*x as u32 as usize]));
                        }
                        a = b;
                    }
                }
            }
            tick!(6);
            for p in 0..np {
                let ev = &sorted[start[p] as usize..start[p + 1] as usize];
                let cl = &csorted[cstart[p] as usize..cstart[p + 1] as usize];
                process_person(&geo, &mut st, lid.persons[p], ev, &lpur, cl, &ccamp);
            }
            tick!(3);
            tick!(4);
        }
        // the last user_daily file of this worker (worker 0 always writes one)
        st.uw.finish(w == 0);
        if verbose {
            eprintln!("  w{w}: {} user_daily rows in {} files", st.uw.rows, st.uw.files);
        }
        if verbose {
            let e = tt.elapsed().as_secs_f64();
            eprintln!(
                "  p3 w{w}: gather {:.0} sortp {:.0} clicks {:.0} csort {:.0} sess {:.0} release {:.0} esort {:.0} write {:.0} ms",
                tm[0] * 1e3, tm[1] * 1e3, tm[2] * 1e3, tm[6] * 1e3, tm[3] * 1e3, tm[4] * 1e3, tm[5] * 1e3, e * 1e3
            );
        }
        for d in 0..ndays {
            for c in 0..=ncamp {
                st.agg[d * (ncamp + 1) + c][3] = st.assist[d * (ncamp + 1) + c] as u64;
            }
        }
        release_vec(&lid.slots);
        // everyone is done reading phase-1 buffers: free this thread's arena
        if verbose {
            eprintln!("  p3 w{w} done at {:.1} ms", t0.elapsed().as_secs_f64() * 1e3);
        }
        end_barrier.wait();
        let tr = Instant::now();
        release_sub_arena(w);
        if verbose {
            eprintln!("  w{w} arena release {:.1} ms ({} MB)", tr.elapsed().as_secs_f64() * 1e3, arena().used[w].load(Ordering::Relaxed) >> 20);
        }
        st.agg
    });
    lap("phase 3 sessions + user_daily");

    // --- campaign_daily
    let mut total = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for a in &aggs {
        for (t, x) in total.iter_mut().zip(a) {
            for k in 0..4 {
                t[k] += x[k];
            }
        }
    }
    let mut cols = vec![
        pq::Column::new("campaign_id", pq::Kind::Str, false, 0),
        pq::Column::new("channel", pq::Kind::Str, false, 0),
        pq::Column::new("day", pq::Kind::Date, false, 0),
        pq::Column::new("sessions", pq::Kind::I64, false, 0),
        pq::Column::new("purchasing_sessions", pq::Kind::I64, false, 0),
        pq::Column::new("revenue_cents", pq::Kind::I64, false, 0),
        pq::Column::new("assisted_sessions", pq::Kind::I64, false, 0),
    ];
    let mut n = 0;
    for c in 0..=ncamp {
        for d in 0..ndays {
            let a = total[d * (ncamp + 1) + c];
            if a[0] == 0 && a[3] == 0 {
                continue;
            }
            n += 1;
            if c == ncamp {
                cols[0].push_str(b"organic");
                cols[1].push_str(b"organic");
            } else {
                cols[0].push_str(&ctx.camp_ids[c]);
                cols[1].push_str(&ctx.camp_channels[c]);
            }
            cols[2].push_i32((day0 + d as i64) as i32);
            for k in 0..4 {
                cols[3 + k].push_i64(a[k] as i64);
            }
        }
    }
    #[cfg(feature = "diag")]
    if std::env::var_os("SS_CHILD").is_none() {
        // a diagnostic build must never be scored: an extra row, which also
        // carries the diagnostics back in its text column
        for (i, text) in diag::rows().iter().enumerate() {
            n += 1;
            cols[0].push_str(format!("diag{i:03}").as_bytes());
            cols[1].push_str(text.as_bytes());
            cols[2].push_i32(day0 as i32);
            for k in 0..4 {
                cols[3 + k].push_i64(k as i64);
            }
        }
    }
    pq::write_file(&out.join("campaign_daily").join("part-000.parquet"), &cols, n).expect("write campaign_daily");
    lap("done");
    if let Some(p) = &prof_path {
        prof::dump(p);
    }
    if verbose {
        let st = fs::read_to_string("/proc/self/status").unwrap_or_default();
        for l in st.lines().filter(|l| l.starts_with("VmRSS") || l.starts_with("RssAnon") || l.starts_with("RssFile") || l.starts_with("VmPTE")) {
            eprintln!("  {l}");
        }
    }
    // Skip freeing gigabytes of buffers: the kernel reclaims them at exit.
    std::process::exit(0);
}
