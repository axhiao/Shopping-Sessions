//! Shopping Sessions pipeline. See README.md for the design.

mod dense;
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
/// How far ahead of the line being scanned the input is prefetched (bytes).
const INPUT_AHEAD: usize = 512;

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
        self.grow_to(self.cap * 2);
    }
    /// Moves to a buffer with room for `nc` records (more than now).
    fn grow_to(&mut self, nc: usize) {
        debug_assert!(nc > self.cap);
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

/// How far ahead a buffer that fills unusually fast is sized (see `presize`).
const PRESIZE_SPARE: f64 = 1.25;
/// A partition is looked at once it holds this many records (far more than
/// an even share of the first chunk of a thread).
const PRESIZE_MIN: usize = 256;

/// The capacity a buffer holding `n` records should get when `factor` times
/// as much input is expected in all, if that is more than twice what it has.
#[inline(always)]
fn presize_want(n: usize, cap: usize, factor: f64) -> Option<usize> {
    if n < PRESIZE_MIN {
        return None;
    }
    let want = (n as f64 * factor * PRESIZE_SPARE) as usize;
    if want > cap * 2 {
        Some(want)
    } else {
        None
    }
}

/// See `Parts::presize`: the same for plain per-partition lists.
fn presize_lists<T: Copy>(lists: &mut [PVec<T>], factor: f64) {
    for o in lists {
        if let Some(want) = presize_want(o.len, o.cap, factor) {
            o.grow_to(want);
        }
    }
}

/// Partitioned output with software write-combining: records are staged in
/// small per-partition buffers and written out K at a time with streaming
/// stores (K * size_of::<T>() is a multiple of 64 bytes).
struct Parts<T: Copy, const K: usize> {
    out: Vec<PVec<T>>,
    buf: Vec<[T; K]>,
    /// Records pushed so far, per partition; the low bits are the fill of the
    /// staging line (K is a power of two).
    cnt: Vec<u32>,
}

impl<T: Copy, const K: usize> Parts<T, K> {
    fn new(parts: usize, reserve: usize) -> Self {
        Self::new_by(parts, |_| reserve)
    }
    /// With room for `reserve(q)` records in partition q.
    fn new_by(parts: usize, reserve: impl Fn(usize) -> usize) -> Self {
        assert!(K * std::mem::size_of::<T>() % 64 == 0 && K.is_power_of_two());
        let mut buf = Vec::with_capacity(parts);
        unsafe {
            buf.set_len(parts);
            std::ptr::write_bytes(buf.as_mut_ptr(), 0, parts);
        }
        Parts { out: (0..parts).map(|q| PVec::with_capacity(reserve(q).div_ceil(K) * K)).collect(), buf, cnt: vec![0; parts] }
    }
    /// Called by a thread once its first chunk of a kind has gone through:
    /// `factor` times as much is expected for it in all. The reserves assume
    /// that records spread evenly over the partitions, but a person with a
    /// great many events fills its partition many times faster; such a
    /// buffer would be outgrown again and again, each time leaving behind a
    /// buffer that was zeroed and copied for nothing. It gets its room now.
    fn presize(&mut self, factor: f64) {
        for q in 0..self.out.len() {
            if let Some(want) = presize_want(self.cnt[q] as usize, self.out[q].cap, factor) {
                self.out[q].grow_to(want.div_ceil(K) * K);
            }
        }
    }
    /// Appends `v` to partition q; returns the index it got there.
    #[inline(always)]
    fn push(&mut self, q: usize, v: T) -> usize {
        unsafe {
            let c = *self.cnt.get_unchecked(q);
            *self.cnt.get_unchecked_mut(q) = c + 1;
            let k = c as usize & (K - 1);
            if k + 1 == K {
                #[cfg(target_feature = "avx2")]
                if std::mem::size_of::<T>() == 16 || std::mem::size_of::<T>() == 32 {
                    self.flush_with(q, v);
                    return c as usize;
                }
                *self.buf.get_unchecked_mut(q).get_unchecked_mut(k) = v;
                self.flush(q);
            } else {
                *self.buf.get_unchecked_mut(q).get_unchecked_mut(k) = v;
            }
            c as usize
        }
    }
    /// Appends `vs` to partition q.
    #[inline(always)]
    fn extend(&mut self, q: usize, vs: &[T]) {
        unsafe {
            let c = *self.cnt.get_unchecked(q) as usize;
            *self.cnt.get_unchecked_mut(q) = (c + vs.len()) as u32;
            let mut k = c & (K - 1);
            let mut i = 0;
            while i < vs.len() {
                let m = (K - k).min(vs.len() - i);
                std::ptr::copy_nonoverlapping(vs.as_ptr().add(i), self.buf.get_unchecked_mut(q).as_mut_ptr().add(k), m);
                i += m;
                k += m;
                if k == K {
                    self.flush(q);
                    k = 0;
                }
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
            _mm256_stream_si256(dst.add(i) as *mut __m256i, x);
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
        _mm256_stream_si256(dst.add(i) as *mut __m256i, last);
        o.len += K;
    }
    #[inline(always)]
    fn flush(&mut self, q: usize) {
        unsafe {
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
                    if NT_STORES {
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
            let k = self.cnt[q] as usize & (K - 1);
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

/// Strings of more than 15 bytes, by their hashed key, so that they can be
/// printed and compared: a lock-free table (a slot is claimed through its
/// first key word; the text and then the second word follow, and the second
/// word publishes the slot). An ID whose stretch of the table is crowded goes
/// to the sharded maps instead.
struct LongSlot {
    k0: AtomicU64,
    k1: AtomicU64,
    ptr: AtomicUsize,
    len: AtomicUsize,
}

/// Slots of the table (a power of two): set from the input size before the
/// first long string is met.
static LONG_SLOTS: AtomicUsize = AtomicUsize::new(1 << 16);
static LONG_TABLE: OnceLock<&'static [LongSlot]> = OnceLock::new();
const LONG_PROBES: usize = 64;
const LONG_SHARDS: usize = 64;
static LONG: [Mutex<BTreeMap<Key, Vec<u8>>>; LONG_SHARDS] = [const { Mutex::new(BTreeMap::new()) }; LONG_SHARDS];

#[inline]
fn long_table() -> &'static [LongSlot] {
    LONG_TABLE.get_or_init(|| {
        let t: &'static mut [LongSlot] = zeroed_slice(LONG_SLOTS.load(Ordering::Relaxed).next_power_of_two());
        t
    })
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

/// The key of a string of more than 15 bytes: an 88-bit hash, 0xFF in the
/// last byte. (The first word is never 0: that marks an empty slot.)
/// The 32 bits above the last byte are left zero on purpose: that is the
/// shape `compact_plain` asks for, so a long key travels in 12 bytes like a
/// short one, with no entry in a side list and no map lookup per record in
/// the session phase. (88 bits: two of a billion long IDs collide with a
/// chance of about 1 in 600 million.)
#[inline(always)]
fn long_hash(s: &[u8]) -> Key {
    let mut h1: u64 = 0x243F_6A88_85A3_08D3 ^ s.len() as u64;
    let mut h2: u64 = 0x1319_8A2E_0370_7344;
    // Eight bytes at a time; the last word is read whole and masked (every
    // buffer of the pipeline has slack past its end; tests copy instead).
    #[cfg(not(test))]
    {
        let (n, p) = (s.len(), s.as_ptr());
        let mut i = 0;
        while i < n {
            let mut x = unsafe { (p.add(i) as *const u64).read_unaligned() };
            if n - i < 8 {
                x &= !0u64 >> (8 * (8 - (n - i)));
            }
            h1 = fm(h1 ^ x, 0x9E37_79B9_7F4A_7C15);
            h2 = fm(h2 ^ x, 0xC2B2_AE3D_27D4_EB4F) ^ h1;
            i += 8;
        }
    }
    #[cfg(test)]
    for c in s.chunks(8) {
        let mut w = [0u8; 8];
        w[..c.len()].copy_from_slice(c);
        let x = u64::from_le_bytes(w);
        h1 = fm(h1 ^ x, 0x9E37_79B9_7F4A_7C15);
        h2 = fm(h2 ^ x, 0xC2B2_AE3D_27D4_EB4F) ^ h1;
    }
    [h1 | (h1 == 0) as u64, (h2 & 0xFFFF_FF00_0000_0000) | 0xFF]
}

/// Where `k` is in the table: its slot if it is there, else None.
fn long_find(k: &Key) -> Option<&'static LongSlot> {
    let t = long_table();
    let mask = t.len() - 1;
    let mut i = (k[0] >> 20) as usize & mask;
    for _ in 0..LONG_PROBES {
        let sl = &t[i];
        let k0 = sl.k0.load(Ordering::Acquire);
        if k0 == 0 {
            return None;
        }
        if k0 == k[0] {
            let mut k1 = sl.k1.load(Ordering::Acquire);
            while k1 == 0 {
                std::hint::spin_loop();
                k1 = sl.k1.load(Ordering::Acquire);
            }
            if k1 == k[1] {
                return Some(sl);
            }
        }
        i = (i + 1) & mask;
    }
    None
}

/// Fetches the slot where `k` is looked for first.
#[inline(always)]
fn long_prefetch(k: &Key) {
    let t = long_table();
    prefetch(&t[(k[0] >> 20) as usize & (t.len() - 1)]);
}

/// Remembers the text of a long string under its key.
fn long_register(k: &Key, s: &[u8]) {
    let t = long_table();
    let mask = t.len() - 1;
    let mut i = (k[0] >> 20) as usize & mask;
    let mut probes = 0;
    while probes < LONG_PROBES {
        let sl = &t[i];
        let k0 = sl.k0.load(Ordering::Acquire);
        if k0 == 0 {
            if sl.k0.compare_exchange(0, k[0], Ordering::AcqRel, Ordering::Acquire).is_ok() {
                let text: &'static [u8] = Box::leak(s.to_vec().into_boxed_slice());
                sl.ptr.store(text.as_ptr() as usize, Ordering::Relaxed);
                sl.len.store(text.len(), Ordering::Relaxed);
                sl.k1.store(k[1], Ordering::Release);
                return;
            }
            // another thread took the slot: look at it again
            continue;
        }
        if k0 == k[0] {
            let mut k1 = sl.k1.load(Ordering::Acquire);
            while k1 == 0 {
                std::hint::spin_loop();
                k1 = sl.k1.load(Ordering::Acquire);
            }
            if k1 == k[1] {
                return;
            }
        }
        i = (i + 1) & mask;
        probes += 1;
    }
    long_shard(k).lock().unwrap().entry(*k).or_insert_with(|| s.to_vec());
}

#[cold]
#[inline(never)]
fn long_key(s: &[u8]) -> Key {
    let k = long_hash(s);
    long_register(&k, s);
    k
}

/// The key of a long string that is only ever compared by its bytes, never
/// for equality (a click ID: copies of a click need no dedup, and only clicks
/// at the same instant are ordered by ID): the place of its bytes, 0xFE in
/// the last byte. `s` must stay where it is until the end (a mapped file) and
/// pass `ref_ok`.
///
/// The key also carries the string's first seven bytes, which settle nearly
/// every comparison: the bytes themselves are in pages that were given back
/// after parsing, and reading them again costs a page fault.
/// Layout: k[0] = address (48 bits) | bytes 0..2 << 48;
/// k[1] = bytes 2..7 << 24 | length (16 bits) << 8 | 0xFE.
#[inline(always)]
fn key_ref(s: &[u8]) -> Key {
    debug_assert!(ref_ok(s));
    let w = unsafe { (s.as_ptr() as *const u64).read_unaligned() }.swap_bytes();
    [s.as_ptr() as u64 | (w >> 48) << 48, ((w >> 8) & 0xFF_FFFF_FFFF) << 24 | (s.len() as u64) << 8 | 0xFE]
}

/// Whether `s` (longer than 15 bytes) can be kept by reference: its address
/// and its length must fit their bits of the key.
#[inline(always)]
fn ref_ok(s: &[u8]) -> bool {
    (s.as_ptr() as u64) >> 48 == 0 && s.len() <= 0xFFFF
}

#[inline(always)]
fn is_ref(k: &Key) -> bool {
    k[1] & 0xFF == 0xFE
}

/// The first seven bytes of the string behind a by-reference key, big-endian.
#[inline(always)]
fn ref_prefix(k: &Key) -> u64 {
    (k[0] >> 48) << 40 | k[1] >> 24
}

/// How the long string whose first seven bytes are `p` compares to the short
/// key `s`, if those bytes settle it.
#[inline(always)]
fn cmp_prefix_short(p: u64, s: &Key) -> Option<std::cmp::Ordering> {
    let n = (s[1] & 0xFF) as u32;
    let s7 = s[0] >> 8;
    if n >= 7 {
        return if p != s7 { Some(p.cmp(&s7)) } else { None };
    }
    // the short string's n bytes against the same bytes of the long one; if
    // they are equal the short one is a prefix of the long one
    let sh = 8 * (7 - n);
    let (pn, sn) = (p >> sh, s7 >> sh);
    Some(if pn != sn { pn.cmp(&sn) } else { std::cmp::Ordering::Greater })
}

/// The bytes of a key that is not short, if they are at hand without a lock.
#[inline]
fn long_slice(k: &Key) -> Option<&'static [u8]> {
    unsafe {
        if is_ref(k) {
            return Some(std::slice::from_raw_parts((k[0] & 0xFFFF_FFFF_FFFF) as *const u8, ((k[1] >> 8) & 0xFFFF) as usize));
        }
        long_find(k).map(|sl| std::slice::from_raw_parts(sl.ptr.load(Ordering::Relaxed) as *const u8, sl.len.load(Ordering::Relaxed)))
    }
}

/// The key of a string whose text is never asked for again (an event ID is
/// only ever compared for equality): a long one is hashed and not remembered.
#[inline(always)]
fn key_of_id(s: &[u8]) -> Key {
    if s.len() > 15 {
        long_hash(s)
    } else {
        key_of(s)
    }
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
    } else if let Some(b) = long_slice(k) {
        out.extend_from_slice(b);
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
    // (strings kept by reference: their first bytes are in the keys)
    match (is_ref(a), is_ref(b)) {
        (true, true) => {
            let (pa, pb) = (ref_prefix(a), ref_prefix(b));
            if pa != pb {
                return pa.cmp(&pb);
            }
        }
        (true, false) if is_short(b) => {
            if let Some(o) = cmp_prefix_short(ref_prefix(a), b) {
                return o;
            }
        }
        (false, true) if is_short(a) => {
            if let Some(o) = cmp_prefix_short(ref_prefix(b), a) {
                return o.reverse();
            }
        }
        _ => {}
    }
    // (the usual case: both texts at hand without copying)
    let short = |k: &Key, buf: &mut [u8; 16]| {
        buf[..8].copy_from_slice(&k[0].to_be_bytes());
        buf[8..].copy_from_slice(&k[1].to_be_bytes());
        (k[1] & 0xFF) as usize
    };
    let (mut ba, mut bb) = ([0u8; 16], [0u8; 16]);
    let sa: Option<&[u8]> = if is_short(a) {
        let n = short(a, &mut ba);
        Some(&ba[..n])
    } else {
        long_slice(a)
    };
    let sb: Option<&[u8]> = if is_short(b) {
        let n = short(b, &mut bb);
        Some(&bb[..n])
    } else {
        long_slice(b)
    };
    if let (Some(x), Some(y)) = (sa, sb) {
        return x.cmp(y);
    }
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

/// Where the fields of one well-formed line are: `e` is the offset of its
/// newline from the line start, `r` the offset just past the K-th separator;
/// bit i of `cs` / `ce` is set where a field of the low side (the first K)
/// starts / ends at byte i, and `csw` / `cew` are the same for the other
/// fields, relative to byte r. Quotes around a field are already excluded.
#[derive(Clone, Copy)]
struct LineCur {
    e: usize,
    r: usize,
    cs: u64,
    ce: u64,
    csw: u64,
    cew: u64,
}

enum Line {
    /// Does not fit the windows: needs the reference scanner.
    Slow,
    /// Not N fields or stray quotes: dropped; the newline is at this offset.
    Bad(usize),
    Good(LineCur),
}

/// Finds the fields of the line at `p` (N fields, the first K within 63
/// bytes, the rest within 64 more). `p` needs WIN readable bytes.
#[inline(always)]
unsafe fn line_cursors<const N: usize, const K: usize>(p: *const u8) -> Line {
    let (sep, nl, qt) = win_masks(p);
    // the line ends at the first newline (128: none in the window)
    let e = if nl[0] != 0 { nl[0].trailing_zeros() } else { 64 + nl[1].trailing_zeros() } as usize;
    // s1: the low separators without the first K - 1; r: just past the K-th
    let mut s1 = sep[0];
    for _ in 1..K {
        s1 &= s1.wrapping_sub(1);
    }
    let r = s1.trailing_zeros() as usize + 1;
    if r > 63 || e.wrapping_sub(r) >= 64 {
        return Line::Slow;
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
    if (bad_lo | bad_hi) != 0 || r + t1.trailing_zeros() as usize != e {
        return Line::Bad(e);
    }
    // first and one-past-last content byte of each field (inside the quotes)
    Line::Good(LineCur { e, r, cs: starts + o, ce: seps - c, csw: startw + ow, cew: sepw - cw })
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
    while ls < limit && ls < n {
        let p = unsafe { ptr.add(ls) };
        // The input is read once, front to back, from 4 KB pages: the hardware
        // prefetcher restarts at every page, and on the scorer the loads below
        // waited for memory (phase 1 is 9% faster with this one hint per line).
        prefetch(p.wrapping_add(INPUT_AHEAD));
        match unsafe { line_cursors::<N, K>(p) } {
            Line::Slow => {
                let (end, res) = slow_line::<N>(buf, ls);
                if let Some(res) = res {
                    call_cold(&mut f, &res);
                }
                ls = end + 1;
            }
            Line::Bad(e) => ls += e + 1,
            Line::Good(c) => {
                let mut fields: [&'a [u8]; N] = [&[]; N];
                let (mut cs, mut ce) = (c.cs, c.ce);
                unsafe {
                    for i in 0..K {
                        let (a, b) = (cs.trailing_zeros() as usize, ce.trailing_zeros() as usize);
                        *fields.get_unchecked_mut(i) = std::slice::from_raw_parts(p.add(a), b.wrapping_sub(a));
                        cs &= cs.wrapping_sub(1);
                        ce &= ce.wrapping_sub(1);
                    }
                    let pr = p.add(c.r);
                    let (mut cs, mut ce) = (c.csw, c.cew);
                    for i in K..N {
                        let (a, b) = (cs.trailing_zeros() as usize, ce.trailing_zeros() as usize);
                        *fields.get_unchecked_mut(i) = std::slice::from_raw_parts(pr.add(a), b.wrapping_sub(a));
                        cs &= cs.wrapping_sub(1);
                        ce &= ce.wrapping_sub(1);
                    }
                }
                f(&fields);
                ls += c.e + 1;
            }
        }
    }
}

/// One side of a line's layout (its first K fields, or the others), as it
/// follows from where that side's separators and quotes are: start and
/// length of each field's content, relative to the side's first byte. The
/// same few hundred patterns make up nearly all lines (every field is quoted
/// or not, and has one of a few lengths), so a side is worked out once per
/// pattern and looked up after that.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
struct Half {
    /// The pattern: separator bits and quote bits of the side (never both 0).
    a: u64,
    b: u64,
    s: [u8; 4],
    l: [u8; 4],
    /// 0: quotes not around whole fields, or not the right number of fields.
    good: u8,
}

const MEMO_N: usize = 2048;

/// The remembered sides of one kind of line (per thread).
struct ShapeMemo {
    lo: [Half; MEMO_N],
    hi: [Half; MEMO_N],
}

impl ShapeMemo {
    fn new() -> Box<ShapeMemo> {
        const E: Half = Half { a: 0, b: 0, s: [0; 4], l: [0; 4], good: 0 };
        Box::new(ShapeMemo { lo: [E; MEMO_N], hi: [E; MEMO_N] })
    }
}

#[cfg(all(target_feature = "bmi2", target_feature = "sse4.2"))]
#[inline(always)]
fn memo_slot(a: u64, b: u64) -> usize {
    use std::arch::x86_64::_mm_crc32_u64;
    unsafe { _mm_crc32_u64(_mm_crc32_u64(0, a), b) as usize & (MEMO_N - 1) }
}

/// The low side from its pattern: `seps` = its K separators, `q` = the quotes
/// before the K-th separator (the same rules as `line_cursors`).
#[cold]
#[inline(never)]
fn half_lo(h: &mut Half, seps: u64, q: u64, k: usize) {
    let top = 1u64 << (63 - seps.leading_zeros());
    let starts = ((seps ^ top) << 1) | 1;
    let (o, c) = (q & starts, q & (seps >> 1));
    let bad = (q ^ o ^ c) | (((!seps).wrapping_add(o) ^ (c << 1)) & seps);
    let (mut cs, mut ce) = (starts + o, seps - c);
    *h = Half { a: seps, b: q, s: [0; 4], l: [0; 4], good: (bad == 0) as u8 };
    for i in 0..k {
        let (x, y) = (cs.trailing_zeros(), ce.trailing_zeros());
        h.s[i] = x as u8;
        h.l[i] = y.wrapping_sub(x) as u8;
        cs &= cs.wrapping_sub(1);
        ce &= ce.wrapping_sub(1);
    }
}

/// The high side from its pattern: `w` = the separators from the side's first
/// byte to the line's newline (its top bit), `qf` = the quotes there; `m`
/// fields are expected.
#[cold]
#[inline(never)]
fn half_hi(h: &mut Half, w: u64, qf: u64, m: usize) {
    let mut t1 = w;
    for _ in 1..m {
        t1 &= t1.wrapping_sub(1);
    }
    // well-formed needs exactly m separators (the last is the newline)
    let count_ok = t1 != 0 && t1 & t1.wrapping_sub(1) == 0;
    let t2 = t1 & t1.wrapping_sub(1);
    let sepw = w ^ t2;
    let startw = ((w ^ t1) << 1) | 1;
    let qw = qf & (t1 ^ t1.wrapping_sub(1));
    let (ow, cw) = (qw & startw, qw & (sepw >> 1));
    let bad = (qw ^ ow ^ cw) | (((!sepw).wrapping_add(ow) ^ (cw << 1)) & sepw);
    let (mut cs, mut ce) = (startw + ow, sepw - cw);
    *h = Half { a: w, b: qf, s: [0; 4], l: [0; 4], good: (bad == 0 && count_ok) as u8 };
    if h.good != 0 {
        for i in 0..m {
            let (x, y) = (cs.trailing_zeros(), ce.trailing_zeros());
            h.s[i] = x as u8;
            h.l[i] = y.wrapping_sub(x) as u8;
            cs &= cs.wrapping_sub(1);
            ce &= ce.wrapping_sub(1);
        }
    }
}

/// What `line_memo` finds.
enum LineM {
    Slow,
    Bad(usize),
    /// newline offset, offset of the high side, the two sides
    Good(usize, usize, *const Half, *const Half),
}

/// `line_cursors` with the two sides looked up by their patterns: the same
/// lines are Slow, Bad and Good, with the same fields. N - K and K at most 4.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2", target_feature = "sse4.2"))]
#[inline(always)]
unsafe fn line_memo<const N: usize, const K: usize>(p: *const u8, memo: &mut ShapeMemo) -> LineM {
    use std::arch::x86_64::_bzhi_u64;
    let (sep, nl, qt) = win_masks(p);
    let e = if nl[0] != 0 { nl[0].trailing_zeros() } else { 64 + nl[1].trailing_zeros() } as usize;
    let mut s1 = sep[0];
    for _ in 1..K {
        s1 &= s1.wrapping_sub(1);
    }
    let r = s1.trailing_zeros() as usize + 1;
    if r > 63 || e.wrapping_sub(r) >= 64 {
        return LineM::Slow;
    }
    // low side: the separators and quotes before byte r
    let (ka, kb) = (_bzhi_u64(sep[0], r as u32), _bzhi_u64(qt[0], r as u32));
    let lo = memo.lo.as_mut_ptr().add(memo_slot(ka, kb));
    if ((*lo).a ^ ka) | ((*lo).b ^ kb) != 0 {
        half_lo(&mut *lo, ka, kb, K);
    }
    // high side: from byte r to the newline
    let n = (e - r + 1) as u32;
    let ha = _bzhi_u64((sep[0] >> r) | (sep[1] << (64 - r)), n);
    let hb = _bzhi_u64((qt[0] >> r) | (qt[1] << (64 - r)), n);
    let hi = memo.hi.as_mut_ptr().add(memo_slot(ha, hb));
    if ((*hi).a ^ ha) | ((*hi).b ^ hb) != 0 {
        half_hi(&mut *hi, ha, hb, N - K);
    }
    if (*lo).good & (*hi).good == 0 {
        return LineM::Bad(e);
    }
    LineM::Good(e, r, lo, hi)
}

/// The masks of `win_masks` for 128 bytes (four loads).
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn win_masks_wide(p: *const u8) -> ([u64; 2], [u64; 2], [u64; 2]) {
    use std::arch::x86_64::*;
    let (nl, cm, qt) = (_mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'"' as i8));
    let m = |x: __m256i| _mm256_movemask_epi8(x) as u32 as u64;
    let mut sep = [0u64; 2];
    let mut nls = [0u64; 2];
    let mut qts = [0u64; 2];
    for k in 0..4 {
        let x = _mm256_loadu_si256(p.add(32 * k) as *const __m256i);
        let n = _mm256_cmpeq_epi8(x, nl);
        sep[k / 2] |= m(_mm256_or_si256(n, _mm256_cmpeq_epi8(x, cm))) << (32 * (k & 1));
        nls[k / 2] |= m(n) << (32 * (k & 1));
        qts[k / 2] |= m(_mm256_cmpeq_epi8(x, qt)) << (32 * (k & 1));
    }
    (sep, nls, qts)
}

/// `line_memo` over a window of 128 bytes instead of 96 (lines with a long
/// ID): the same rules.
/// `line_cursors` with the two sides looked up by their patterns: the same
/// lines are Slow, Bad and Good, with the same fields. N - K and K at most 4.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2", target_feature = "sse4.2"))]
#[inline(always)]
unsafe fn line_memo_wide<const N: usize, const K: usize>(p: *const u8, memo: &mut ShapeMemo) -> LineM {
    use std::arch::x86_64::_bzhi_u64;
    let (sep, nl, qt) = win_masks_wide(p);
    let e = if nl[0] != 0 { nl[0].trailing_zeros() } else { 64 + nl[1].trailing_zeros() } as usize;
    let mut s1 = sep[0];
    for _ in 1..K {
        s1 &= s1.wrapping_sub(1);
    }
    let r = s1.trailing_zeros() as usize + 1;
    if r > 63 || e.wrapping_sub(r) >= 64 {
        return LineM::Slow;
    }
    // low side: the separators and quotes before byte r
    let (ka, kb) = (_bzhi_u64(sep[0], r as u32), _bzhi_u64(qt[0], r as u32));
    let lo = memo.lo.as_mut_ptr().add(memo_slot(ka, kb));
    if ((*lo).a ^ ka) | ((*lo).b ^ kb) != 0 {
        half_lo(&mut *lo, ka, kb, K);
    }
    // high side: from byte r to the newline
    let n = (e - r + 1) as u32;
    let ha = _bzhi_u64((sep[0] >> r) | (sep[1] << (64 - r)), n);
    let hb = _bzhi_u64((qt[0] >> r) | (qt[1] << (64 - r)), n);
    let hi = memo.hi.as_mut_ptr().add(memo_slot(ha, hb));
    if ((*hi).a ^ ha) | ((*hi).b ^ hb) != 0 {
        half_hi(&mut *hi, ha, hb, N - K);
    }
    if (*lo).good & (*hi).good == 0 {
        return LineM::Bad(e);
    }
    LineM::Good(e, r, lo, hi)
}

/// Fields from the remembered sides of a line.
struct MemoFields<'a> {
    p: *const u8,
    pr: *const u8,
    lo: *const Half,
    hi: *const Half,
    i: usize,
    j: usize,
    _buf: std::marker::PhantomData<&'a [u8]>,
}

impl<'a> Fields<'a> for MemoFields<'a> {
    #[inline(always)]
    fn lo(&mut self) -> &'a [u8] {
        let k = self.i;
        self.i += 1;
        unsafe { std::slice::from_raw_parts(self.p.add(*(*self.lo).s.get_unchecked(k) as usize), *(*self.lo).l.get_unchecked(k) as usize) }
    }
    #[inline(always)]
    fn hi(&mut self) -> &'a [u8] {
        let k = self.j;
        self.j += 1;
        unsafe { std::slice::from_raw_parts(self.pr.add(*(*self.hi).s.get_unchecked(k) as usize), *(*self.hi).l.get_unchecked(k) as usize) }
    }
}

/// The fields of a line, handed out in order: the first K with `lo`, the
/// others with `hi`.
trait Fields<'a> {
    fn lo(&mut self) -> &'a [u8];
    fn hi(&mut self) -> &'a [u8];
}

/// Fields taken one at a time from the cursors of a line (nothing is
/// computed for a field before it is asked for, so few values stay live).
struct MaskFields<'a> {
    p: *const u8,
    pr: *const u8,
    cs: u64,
    ce: u64,
    csw: u64,
    cew: u64,
    _buf: std::marker::PhantomData<&'a [u8]>,
}

impl<'a> MaskFields<'a> {
    #[inline(always)]
    unsafe fn new(p: *const u8, c: &LineCur) -> Self {
        MaskFields { p, pr: p.add(c.r), cs: c.cs, ce: c.ce, csw: c.csw, cew: c.cew, _buf: std::marker::PhantomData }
    }
}

impl<'a> Fields<'a> for MaskFields<'a> {
    #[inline(always)]
    fn lo(&mut self) -> &'a [u8] {
        let (a, b) = (self.cs.trailing_zeros() as usize, self.ce.trailing_zeros() as usize);
        self.cs &= self.cs.wrapping_sub(1);
        self.ce &= self.ce.wrapping_sub(1);
        unsafe { std::slice::from_raw_parts(self.p.add(a), b.wrapping_sub(a)) }
    }
    #[inline(always)]
    fn hi(&mut self) -> &'a [u8] {
        let (a, b) = (self.csw.trailing_zeros() as usize, self.cew.trailing_zeros() as usize);
        self.csw &= self.csw.wrapping_sub(1);
        self.cew &= self.cew.wrapping_sub(1);
        unsafe { std::slice::from_raw_parts(self.pr.add(a), b.wrapping_sub(a)) }
    }
}

/// Fields from an array (lines that went through the reference scanner).
struct ArrFields<'a, 'b, const N: usize> {
    f: &'b [&'a [u8]; N],
    i: usize,
}

impl<'a, 'b, const N: usize> Fields<'a> for ArrFields<'a, 'b, N> {
    #[inline(always)]
    fn lo(&mut self) -> &'a [u8] {
        self.i += 1;
        self.f[self.i - 1]
    }
    #[inline(always)]
    fn hi(&mut self) -> &'a [u8] {
        self.lo()
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
    if let Some(c) = compact_plain(k) {
        c
    } else {
        let i = esc.len() as u32;
        esc.push(*k);
        [i, 0, ESCAPE]
    }
}

/// The compact form of a key that has one (the same key always gives the same
/// form, and different keys different forms).
#[inline(always)]
fn compact_plain(k: &Key) -> Option<Ck> {
    if (k[1] >> 8) & 0xFFFF_FFFF == 0 {
        Some([(k[0] >> 32) as u32, k[0] as u32, (((k[1] >> 32) as u32) & 0xFFFF_FF00) | (k[1] as u32 & 0xFF)])
    } else {
        None
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
// Sequence numbers behind generated IDs.
//
// The dataset generator writes an event ID as 'e' plus the base-36 digits of
// scramble(n, key): n is the event's sequence number and scramble a fixed
// bijection on 40-bit numbers, keyed per dataset. Inverting it gives every
// such ID a small dense number, so those events are deduplicated by direct
// addressing instead of hashing, and nothing about a user has to be looked
// up while parsing. Correctness never depends on any of this: an ID's number
// is an exact, one-to-one function of its text whatever the key is; an ID
// that is not of this form, or whose number is too large, takes the general
// path; and people still come from the links file.

const M40: u64 = (1 << 40) - 1;
const SCR_C1: u64 = 0x9E37_79B9_7F4B;
const SCR_C2: u64 = 0xD6E8_FEB8_6659;

/// Inverse of an odd number modulo 2^64 (so also modulo 2^40).
const fn inv64(a: u64) -> u64 {
    let mut x = a;
    let mut i = 0;
    while i < 6 {
        x = x.wrapping_mul(2u64.wrapping_sub(a.wrapping_mul(x)));
        i += 1;
    }
    x
}

const SCR_C1I: u64 = inv64(SCR_C1);
const SCR_C2I: u64 = inv64(SCR_C2);

/// The generator's bijection on 0..2^40.
fn scramble(n: u64, key: u64) -> u64 {
    let mut m = ((n & M40).wrapping_mul(SCR_C1) ^ key) & M40;
    m ^= m >> 17;
    m.wrapping_mul(SCR_C2) & M40
}

/// scramble(n, key) -> (n * C1) ^ key, the part of the inverse without the key.
#[inline(always)]
fn unmix(v: u64) -> u64 {
    let x = v.wrapping_mul(SCR_C2I) & M40;
    x ^ (x >> 17) ^ (x >> 34)
}

/// The inverse of `scramble` for a key: a bijection on 0..2^40 for any key.
#[inline(always)]
fn seq_of(v: u64, key: u64) -> u64 {
    (unmix(v) ^ key).wrapping_mul(SCR_C1I) & M40
}

/// For an ID of n digits: the shift that puts its last digit in the top byte,
/// and '0' in the bytes below its first.
static B36_SHIFT: [u8; 9] = [0, 56, 48, 40, 32, 24, 16, 8, 0];
static B36_PAD: [u64; 9] = {
    let mut t = [0u64; 9];
    let mut n = 1;
    while n < 8 {
        t[n] = 0x3030_3030_3030_3030u64 >> (8 * n);
        n += 1;
    }
    t
};

/// The number written in `s` as base-36 digits (0-9, a-z) the way the
/// generator prints one: 1 to 8 digits, no leading zero, below 2^40. Two
/// accepted strings are equal exactly when their numbers are. `s` needs 8
/// readable bytes from its start.
#[inline(always)]
fn base36_40(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n.wrapping_sub(1) >= 8 {
        return None;
    }
    let x = unsafe { (s.as_ptr() as *const u64).read_unaligned() };
    if x as u8 == b'0' && n != 1 {
        return None;
    }
    #[cfg(target_feature = "ssse3")]
    let v = unsafe {
        use std::arch::x86_64::*;
        // the digits moved to the top bytes (the last one in byte 7), '0' below them
        let y = (x << B36_SHIFT[n]) | B36_PAD[n];
        let c = _mm_cvtsi64_si128(y as i64);
        let alpha = _mm_cmpgt_epi8(c, _mm_set1_epi8(0x60));
        let d = _mm_sub_epi8(c, _mm_add_epi8(_mm_set1_epi8(0x30), _mm_and_si128(alpha, _mm_set1_epi8(0x27))));
        // a digit is at most 9 (below 'a') or 35 (from 'a' on)
        let lim = _mm_add_epi8(_mm_set1_epi8(9), _mm_and_si128(alpha, _mm_set1_epi8(26)));
        if _mm_cvtsi128_si64(_mm_subs_epu8(d, lim)) != 0 {
            return None;
        }
        let pairs = _mm_maddubs_epi16(d, _mm_set1_epi16(0x0124)); // 36 * even byte + odd byte
        let quads = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_0510)); // 1296 * even word + odd word
        let r = _mm_cvtsi128_si64(quads) as u64;
        (r & 0xFFFF_FFFF) * 1_679_616 + (r >> 32)
    };
    #[cfg(not(target_feature = "ssse3"))]
    let v = {
        let mut v = 0u64;
        for &c in s {
            let d = match c {
                b'0'..=b'9' => c - b'0',
                b'a'..=b'z' => c - b'a' + 10,
                _ => return None,
            };
            v = v * 36 + d as u64;
        }
        v
    };
    if v > M40 {
        return None;
    }
    Some(v)
}

/// The number of an ID made of `letter` and base-36 digits (see `base36_40`).
/// `s` needs 16 readable bytes from its start.
#[inline(always)]
fn id_number(letter: u8, s: &[u8]) -> Option<u64> {
    if s.len() < 2 || unsafe { *s.as_ptr() } != letter {
        return None;
    }
    base36_40(unsafe { std::slice::from_raw_parts(s.as_ptr().add(1), s.len() - 1) })
}

/// A 'u' ID of up to 9 bytes whose other bytes are digits or lower-case
/// letters, packed in 52 bits: six bits per byte (bits 0-4 and 6 of the byte:
/// that tells every such byte apart), the number of bytes after the 'u' in
/// bits 48-51. Exact: `user_code_key` gives the ID's key back. `s` needs 16
/// readable bytes from its start.
#[inline(always)]
fn user_code(s: &[u8]) -> Option<u64> {
    let n = s.len().wrapping_sub(1);
    if n.wrapping_sub(1) >= 8 || unsafe { *s.as_ptr() } != b'u' {
        return None;
    }
    let x = unsafe { (s.as_ptr().add(1) as *const u64).read_unaligned() };
    // '0' in the bytes past the ID
    let hi = (!0u64 << 1) << (8 * n - 1);
    let y = (x & !hi) | (0x3030_3030_3030_3030 & hi);
    #[cfg(all(target_feature = "ssse3", target_feature = "bmi2"))]
    unsafe {
        use std::arch::x86_64::*;
        let c = _mm_cvtsi64_si128(y as i64);
        let alpha = _mm_cmpgt_epi8(c, _mm_set1_epi8(0x60));
        let d = _mm_sub_epi8(c, _mm_add_epi8(_mm_set1_epi8(0x30), _mm_and_si128(alpha, _mm_set1_epi8(0x27))));
        let lim = _mm_add_epi8(_mm_set1_epi8(9), _mm_and_si128(alpha, _mm_set1_epi8(26)));
        if _mm_cvtsi128_si64(_mm_subs_epu8(d, lim)) != 0 {
            return None;
        }
        Some(_pext_u64(y, 0x5F5F_5F5F_5F5F_5F5F) | (n as u64) << 48)
    }
    #[cfg(not(all(target_feature = "ssse3", target_feature = "bmi2")))]
    {
        let mut code = 0u64;
        for i in 0..8 {
            let c = (y >> (8 * i)) as u8;
            if !(c.is_ascii_digit() || c.is_ascii_lowercase()) {
                return None;
            }
            code |= (((c & 0x1F) | ((c & 0x40) >> 1)) as u64) << (6 * i);
        }
        Some(code | (n as u64) << 48)
    }
}

/// The key of the ID packed by `user_code`.
#[inline(always)]
fn user_code_key(code: u64) -> Key {
    let n = (code >> 48) as usize;
    #[cfg(target_feature = "bmi2")]
    let y = unsafe { std::arch::x86_64::_pdep_u64(code, 0x5F5F_5F5F_5F5F_5F5F) } | 0x2020_2020_2020_2020;
    #[cfg(not(target_feature = "bmi2"))]
    let y = {
        let mut y = 0u64;
        for i in 0..8 {
            let v = (code >> (6 * i)) as u8 & 0x3F;
            y |= (((v & 0x1F) | ((v & 0x20) << 1) | 0x20) as u64) << (8 * i);
        }
        y
    };
    let mut buf = [0u8; 32];
    buf[0] = b'u';
    buf[1..9].copy_from_slice(&y.to_le_bytes());
    key_of(&buf[..n + 1])
}

/// The key under which the sampled ID numbers have sequence numbers below
/// `bound`, if there is one. An ID that is not a generated one must not hide
/// the key: up to one sample in eight may fall outside, and four pairs of
/// samples take turns as the anchor.
fn find_seq_key(sample: &[u64], bound: u64, nthreads: usize) -> Option<u64> {
    if sample.len() < 8 || bound < 2 {
        return None;
    }
    let ms: Vec<u64> = sample.iter().map(|&v| unmix(v)).collect();
    let allowed = ms.len() / 8;
    (0..4).find_map(|pair| seq_key_from(&ms, 2 * pair, bound, allowed, nthreads))
}

/// Tries every number for sample `a` (and keeps the keys that also put sample
/// `a + 1` below `bound`); the smallest key that leaves at most `allowed`
/// samples outside.
fn seq_key_from(ms: &[u64], a: usize, bound: u64, allowed: usize, nthreads: usize) -> Option<u64> {
    let (ma, delta) = (ms[a], ms[a] ^ ms[a + 1]);
    let found = AtomicU64::new(u64::MAX);
    let per = bound.div_ceil(nthreads as u64);
    std::thread::scope(|sc| {
        for t in 0..nthreads as u64 {
            let found = &found;
            sc.spawn(move || {
                let (lo, hi) = ((t * per).min(bound), ((t + 1) * per).min(bound));
                // x = n * C1 for the candidate number n of the anchor
                let mut x = lo.wrapping_mul(SCR_C1) & M40;
                let mut n = lo;
                while n < hi {
                    let stop = (n + (1 << 16)).min(hi);
                    while n < stop {
                        // with key = ma ^ x, the next sample's number is
                        if (delta ^ x).wrapping_mul(SCR_C1I) & M40 < bound {
                            let key = ma ^ x;
                            let outside = ms.iter().filter(|&&m| (m ^ key).wrapping_mul(SCR_C1I) & M40 >= bound).count();
                            if outside <= allowed {
                                found.fetch_min(key, Ordering::Relaxed);
                            }
                        }
                        x = x.wrapping_add(SCR_C1) & M40;
                        n += 1;
                    }
                    if found.load(Ordering::Relaxed) != u64::MAX {
                        break;
                    }
                }
            });
        }
    });
    let k = found.load(Ordering::Relaxed);
    (k != u64::MAX).then_some(k)
}

/// How events with a sequence number are laid out: partition `n >> shift`
/// (of `parts`), slot `n & mask` in it. Off (`on` false) when no key fits.
#[derive(Clone, Copy)]
struct IxCfg {
    on: bool,
    key: u64,
    shift: u32,
    mask: u64,
    parts: usize,
    /// Sequence numbers from here on take the general path.
    imax: u64,
}

/// An event on the sequence-number path (16 bytes), partitioned by number.
/// a: bits 0..16 the slot, 16..40 ingest (as in RecD), 40..64 user (high 24
/// bits). b: bits 0..28 user (low 28), 28..59 ts (ms from the window start),
/// then the flags. `user` is a `user_code`, or with X_ODD the index of the
/// user's key in the partition's side list. A purchase is followed by one
/// more record: a = price | category << 32.
#[derive(Clone, Copy)]
#[repr(C)]
struct RecX {
    a: u64,
    b: u64,
}

const X_PURCHASE: u64 = 1 << 59;
const X_VALID: u64 = 1 << 60;
const X_ODD: u64 = 1 << 61;

impl RecX {
    #[inline(always)]
    fn slot(&self) -> usize {
        (self.a & 0xFFFF) as usize
    }
    #[inline(always)]
    fn ingest(&self) -> u64 {
        (self.a >> 16) & 0xFF_FFFF
    }
    #[inline(always)]
    fn user(&self) -> u64 {
        (self.a >> 40) << 28 | (self.b & 0xFFF_FFFF)
    }
    #[inline(always)]
    fn ts(&self) -> u32 {
        ((self.b >> 28) & 0x7FFF_FFFF) as u32
    }
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
    /// Campaigns named `c_` + a number, by that number (0xFFFF: none); empty
    /// if there are too many campaigns for 16 bits.
    camp_direct: Vec<u16>,
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
        // read once, front to back: the kernel need not track which pages were used
        madvise(p, len, 2 /* MADV_SEQUENTIAL */);
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
        // (The page tables are filled by the faults of the scan itself: on the
        // scorer that is 1.4% faster than MADV_POPULATE_READ per chunk, for
        // the sequence path and the general one alike.)
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
    ix: Parts<RecX, 4>,          // by sequence-number partition
    ixodd: Vec<PVec<Key>>,       // per sequence-number partition: users that are not 'u' IDs
}

struct P1Out {
    dd_region: (usize, usize),
    dd: Vec<PVec<RecD>>,
    pay: Vec<PVec<RecP>>,
    pur: Vec<PVec<(u32, u32)>>,
    pesc: Vec<PVec<Key>>,
    clq: Vec<PVec<RecCc>>,
    cesc: Vec<PVec<Key>>,
    ix_region: (usize, usize),
    ix: Vec<PVec<RecX>>,
    ixodd: Vec<PVec<Key>>,
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

/// One parsed event waiting in the batch (one cache line). The parser writes
/// each part as soon as it has it, so nothing is kept in registers or on the
/// stack across the rest of the line.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct EvSlot {
    /// The user as written; resolved to the person when the batch is flushed.
    person: Key,
    /// Hash of the user key.
    h: u64,
    dd: RecD,
    /// (price, category) of a purchase.
    pc: (u32, u32),
    /// Where the user ID is if it is a long one (`ulen` its length, else 0):
    /// the text of a long ID that names its person is remembered when the
    /// batch is flushed, by which time its slot in the table has been fetched.
    uptr: *const u8,
    /// Event time, ms from the window start; bit 31: a purchase.
    ts: u32,
    ulen: u16,
    linked: bool,
}

/// An entry of a parse batch: the hash of its user key and whether the user
/// may be in the link map.
trait BatchSlot: Copy {
    fn hash(&self) -> u64;
    fn set_linked(&mut self, linked: bool);
}

impl BatchSlot for EvSlot {
    #[inline(always)]
    fn hash(&self) -> u64 {
        self.h
    }
    #[inline(always)]
    fn set_linked(&mut self, linked: bool) {
        self.linked = linked;
    }
}

/// Pipeline stage 2: the filter words were prefetched while parsing; decide
/// which users need a probe and prefetch their slots.
#[inline(always)]
fn stage2<S: BatchSlot>(base: *mut S, n: usize, lm: &LinkMap) {
    for j in 0..n {
        let e = unsafe { &mut *base.add(j) };
        let linked = lm.maybe_linked(e.hash());
        if linked {
            lm.prefetch(e.hash());
        }
        e.set_linked(linked);
    }
}

/// Two parse batches of EB entries: one fills while the other waits for the
/// link-map slots prefetched for it. Points into storage owned by the caller.
struct BatchQueue<S> {
    /// Where the next entry is parsed; one past the end of the filling batch.
    slot: *mut S,
    end: *mut S,
    /// The filling batch and the waiting one, with the latter's entry count.
    fill: *mut S,
    wait: *mut S,
    nwait: usize,
}

impl<S: BatchSlot> BatchQueue<S> {
    fn new(store: &mut [[S; EB]; 2]) -> Self {
        let fill = store[0].as_mut_ptr();
        BatchQueue { slot: fill, end: unsafe { fill.add(EB) }, fill, wait: store[1].as_mut_ptr(), nwait: 0 }
    }
    /// Keeps the entry just written to `slot`. When that fills the batch, the
    /// waiting batch is returned: the caller must flush it before parsing on
    /// (its storage is the next one to fill).
    #[inline(always)]
    fn keep(&mut self, lm: &LinkMap) -> Option<(*const S, usize)> {
        self.slot = unsafe { self.slot.add(1) };
        if self.slot != self.end {
            return None;
        }
        stage2(self.fill, EB, lm);
        let ready = (self.wait as *const S, self.nwait);
        std::mem::swap(&mut self.fill, &mut self.wait);
        self.nwait = EB;
        self.slot = self.fill;
        self.end = unsafe { self.fill.add(EB) };
        Some(ready)
    }
    /// The batches still to flush at the end of the input, in order.
    fn rest(&mut self, lm: &LinkMap) -> [(*const S, usize); 2] {
        let n = unsafe { self.slot.offset_from(self.fill) } as usize;
        stage2(self.fill, n, lm);
        [(self.wait as *const S, self.nwait), (self.fill as *const S, n)]
    }
}

#[inline(always)]
fn flush_batch(base: *const EvSlot, n: usize, lm: &LinkMap, o: &mut P1Work) {
    let (pc_shift, pa_shift) = (64 - pc_bits(), 64 - pa_bits());
    for j in 0..n {
        let e = unsafe { &*base.add(j) };
        let mut person = e.person;
        let mut ph = e.h;
        if e.linked {
            person = lm.person(&person, ph);
            ph = hk(&person);
        }
        // (a linked ID's person is named by an ID of the links, remembered there)
        if e.ulen != 0 && person == e.person {
            long_register(&person, unsafe { std::slice::from_raw_parts(e.uptr, e.ulen as usize) });
        }
        let q = (ph >> pc_shift) as usize;
        if e.ts >> 31 != 0 {
            unsafe { o.pur.get_unchecked_mut(q) }.push(e.pc);
        }
        let rec = RecP { p: compact(&person, unsafe { o.pesc.get_unchecked_mut(q) }), t: e.ts };
        let mut d = e.dd;
        let idx = o.pay.push(q, rec) as u64;
        if idx >= 1 << IDX_BITS {
            IDX_OVERFLOW.store(true, Ordering::Relaxed);
        }
        d.b |= HAS_PAY | ((q as u64) << IDX_BITS) | (idx & ((1 << IDX_BITS) - 1));
        o.dd.push((d.fp >> pa_shift) as usize, d);
    }
}

/// What an event line is: a copy that only takes part in the dedup (anything
/// invalid), or a valid event.
const EV_DEDUP: u32 = 1;
const EV_VALID: u32 = 2;

/// Parses the fields of an event line after its event id `f0` (not empty),
/// in order, into `slot`. `dd` is complete for EV_DEDUP and EV_VALID; the
/// rest only matters for EV_VALID.
#[inline(always)]
unsafe fn event_tail<'a, S: Fields<'a>>(ctx: &Ctx, dc: &mut DateCache, lm: &LinkMap, f0: &[u8], src: &mut S, slot: *mut EvSlot) -> u32 {
    let (fp, fp2) = eid_fp(&key_of_id(f0));
    (*slot).dd.fp = fp;
    // user_id
    let f1 = src.lo();
    let mut ok = !f1.is_empty();
    // (the key of an empty string is ZERO)
    // (a long ID is hashed here and remembered at the flush, see EvSlot)
    let user = if f1.len().wrapping_sub(16) < 65_520 {
        let k = long_hash(f1);
        long_prefetch(&k);
        (*slot).uptr = f1.as_ptr();
        (*slot).ulen = f1.len() as u16;
        k
    } else {
        (*slot).ulen = 0;
        key_of(f1)
    };
    (*slot).person[0] = user[0];
    (*slot).person[1] = user[1];
    let h = hk(&user);
    (*slot).h = h;
    lm.prefetch_filter(h);
    // ts: inside the window
    let f2 = src.lo();
    let ts = match parse_ts_dc(dc, f2) {
        Some(t) if t >= ctx.ws && t < ctx.we => t,
        _ => {
            ok = false;
            ctx.ws
        }
    };
    (*slot).ts = (ts - ctx.ws) as u32;
    // ingest_ts: the dedup order; an event more than a day late is dropped
    let f3 = src.hi();
    let (ingest_s, ingest_ms) = parse_ingest_dc(dc, f3).unwrap_or((i64::MAX / 2000, i64::MAX / 2));
    let ingest = (ingest_s - ctx.ingest_base_s).clamp(0, 0xFF_FFFF) as u64;
    (*slot).dd.b = ((fp2 as u64) << 56) | (ingest << 32);
    ok &= ingest_ms - ts <= DAY;
    // event_type, product_id, price_cents
    let ty = event_type(src.hi());
    let f5 = src.hi();
    ok &= !f5.is_empty();
    let f6 = src.hi();
    let mut pc = (0u32, 0u32);
    if ty == T_PURCHASE {
        match parse_price(f6) {
            Some(p) if ok => pc = (p, ctx.products.get(&key_of(f5)).unwrap_or(ctx.unknown_cat)),
            _ => ok = false,
        }
    } else {
        ok &= ty < T_PURCHASE;
    }
    (*slot).ts |= ((ty == T_PURCHASE) as u32) << 31;
    (*slot).pc = pc;
    if ok {
        EV_VALID
    } else {
        EV_DEDUP
    }
}

/// The same for an event with sequence number `n`: one record (two for a
/// purchase) to its number's partition, nothing looked up.
#[inline(always)]
unsafe fn event_ix<'a, S: Fields<'a>>(ctx: &Ctx, ix: &IxCfg, dc: &mut DateCache, n: u64, src: &mut S, o: &mut P1Work) {
    let part = (n >> ix.shift) as usize;
    let f1 = src.lo();
    let mut ok = !f1.is_empty();
    let f2 = src.lo();
    let ts = match parse_ts_dc(dc, f2) {
        Some(t) if t >= ctx.ws && t < ctx.we => t,
        _ => {
            ok = false;
            ctx.ws
        }
    };
    let f3 = src.hi();
    let (ingest_s, ingest_ms) = parse_ingest_dc(dc, f3).unwrap_or((i64::MAX / 2000, i64::MAX / 2));
    let ingest = (ingest_s - ctx.ingest_base_s).clamp(0, 0xFF_FFFF) as u64;
    ok &= ingest_ms - ts <= DAY;
    let ty = event_type(src.hi());
    let f5 = src.hi();
    ok &= !f5.is_empty();
    let f6 = src.hi();
    let mut pc = (0u32, 0u32);
    if ty == T_PURCHASE {
        match parse_price(f6) {
            Some(p) if ok => pc = (p, ctx.products.get(&key_of(f5)).unwrap_or(ctx.unknown_cat)),
            _ => ok = false,
        }
    } else {
        ok &= ty < T_PURCHASE;
    }
    let a = (n & ix.mask) | (ingest << 16);
    if !ok {
        // takes part in the dedup only
        o.ix.push(part, RecX { a, b: 0 });
        return;
    }
    let (user, odd) = match user_code(f1) {
        Some(v) => (v, 0),
        None => {
            let list = o.ixodd.get_unchecked_mut(part);
            list.push(key_of(f1));
            ((list.len() - 1) as u64, X_ODD)
        }
    };
    let purchase = if ty == T_PURCHASE { X_PURCHASE } else { 0 };
    let b = (user & 0xFFF_FFFF) | (((ts - ctx.ws) as u64) << 28) | purchase | X_VALID | odd;
    o.ix.push(part, RecX { a: a | ((user >> 28) << 40), b });
    if purchase != 0 {
        o.ix.push(part, RecX { a: pc.0 as u64 | (pc.1 as u64) << 32, b: 0 });
    }
}

/// One event line: by sequence number if its id has one, else the general way.
#[inline(always)]
unsafe fn event_line<'a, S: Fields<'a>>(
    ctx: &Ctx,
    ix: &IxCfg,
    dc: &mut DateCache,
    lm: &LinkMap,
    src: &mut S,
    q: &mut EvQueue,
    o: &mut P1Work,
) {
    let f0 = src.lo();
    if f0.is_empty() {
        return;
    }
    if ix.on {
        if let Some(v) = id_number(b'e', f0) {
            let n = seq_of(v, ix.key);
            if n < ix.imax {
                event_ix(ctx, ix, dc, n, src, o);
                return;
            }
        }
    }
    let kind = event_tail(ctx, dc, lm, f0, src, q.slot);
    event_commit(q, kind, lm, o);
}

type EvQueue = BatchQueue<EvSlot>;

const EV_EMPTY: EvSlot = EvSlot { person: ZERO, h: 0, dd: RecD { fp: 0, b: 0 }, pc: (0, 0), uptr: std::ptr::null(), ts: 0, ulen: 0, linked: false };
const _: () = assert!(std::mem::size_of::<EvSlot>() == 64);

/// Takes what `event_fields` left in the queue's slot.
#[inline(always)]
fn event_commit(q: &mut EvQueue, kind: u32, lm: &LinkMap, o: &mut P1Work) {
    if kind == EV_VALID {
        if let Some((base, n)) = q.keep(lm) {
            flush_batch(base, n, lm, o);
        }
    } else if kind == EV_DEDUP {
        let d = unsafe { (*q.slot).dd };
        o.dd.push((d.fp >> (64 - pa_bits())) as usize, d);
    }
}

/// A line that did not fit the scanner's windows: reference scanner.
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn event_slow(ctx: &Ctx, ix: &IxCfg, buf: &[u8], ls: usize, dc: &mut DateCache, lm: &LinkMap, q: &mut EvQueue, o: &mut P1Work) -> usize {
    let (end, res) = slow_line::<7>(buf, ls);
    if let Some(f) = res {
        unsafe { event_line(ctx, ix, dc, lm, &mut ArrFields { f: &f, i: 0 }, q, o) };
    }
    end + 1
}

/// The first field of the line at `p` when it ends within the first 63 bytes
/// and is plainly well-formed (no quote in it, or one at each end): its
/// content and the offset just past its separator. None for anything else.
#[cfg(all(target_feature = "avx2", target_feature = "bmi2"))]
#[inline(always)]
unsafe fn first_field<'a>(p: *const u8) -> Option<(&'a [u8], usize)> {
    use std::arch::x86_64::*;
    let a = _mm256_loadu_si256(p as *const __m256i);
    let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
    let m = |x: __m256i, y: __m256i, c: u8| {
        let v = _mm256_set1_epi8(c as i8);
        (_mm256_movemask_epi8(_mm256_cmpeq_epi8(x, v)) as u32 as u64) | (_mm256_movemask_epi8(_mm256_cmpeq_epi8(y, v)) as u32 as u64) << 32
    };
    let (cm, nl, qt) = (m(a, b, b','), m(a, b, b'\n'), m(a, b, b'"'));
    let c0 = cm.trailing_zeros() as usize;
    // the separator must be a comma before any newline, within 63 bytes
    if c0 > 62 || nl & ((1u64 << c0) - 1) != 0 {
        return None;
    }
    let q = qt & ((1u64 << c0) - 1);
    if q == 0 {
        Some((std::slice::from_raw_parts(p, c0), c0 + 1))
    } else if c0 >= 2 && q == 1 | 1 << (c0 - 1) {
        Some((std::slice::from_raw_parts(p.add(1), c0 - 2), c0 + 1))
    } else {
        None
    }
}

/// A first field handed out before the fields of the rest of the line.
struct Lead<'a, S: Fields<'a>> {
    first: Option<&'a [u8]>,
    rest: S,
}

impl<'a, S: Fields<'a>> Fields<'a> for Lead<'a, S> {
    #[inline(always)]
    fn lo(&mut self) -> &'a [u8] {
        match self.first.take() {
            Some(f) => f,
            None => self.rest.lo(),
        }
    }
    #[inline(always)]
    fn hi(&mut self) -> &'a [u8] {
        self.rest.hi()
    }
}

/// An event line whose first three fields do not fit the scanner's window
/// (a long ID): the first field is taken off and the rest scanned as a line
/// of six fields. Returns the next line's start, or None if that does not
/// work either.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
unsafe fn event_long(ctx: &Ctx, ix: &IxCfg, p: *const u8, ls: usize, dc: &mut DateCache, lm: &LinkMap, q: &mut EvQueue, o: &mut P1Work, memo6: &mut ShapeMemo) -> Option<usize> {
    let (f0, off) = first_field(p)?;
    let pt = p.add(off);
    match line_memo_wide::<6, 2>(pt, memo6) {
        LineM::Slow => None,
        LineM::Bad(e) => Some(ls + off + e + 1),
        LineM::Good(e, r, lo, hi) => {
            let mut f = Lead { first: Some(f0), rest: MemoFields { p: pt, pr: pt.add(r), lo, hi, i: 0, j: 0, _buf: std::marker::PhantomData } };
            event_line(ctx, ix, dc, lm, &mut f, q, o);
            Some(ls + off + e + 1)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_events(ctx: &Ctx, ix: &IxCfg, buf: &[u8], begin: usize, limit: usize, lm: &LinkMap, o: &mut P1Work, memo: &mut ShapeMemo, memo6: &mut ShapeMemo) {
    let n = buf.len();
    if begin >= n {
        return;
    }
    debug_assert!(buf[n - 1] == b'\n');
    let mut store = [[EV_EMPTY; EB]; 2];
    let mut q = EvQueue::new(&mut store);
    let mut dc = DateCache::new();
    let ptr = buf.as_ptr();
    let mut ls = begin;
    while ls < limit && ls < n {
        let p = unsafe { ptr.add(ls) };
        // (see scan_lean)
        prefetch(p.wrapping_add(INPUT_AHEAD));
        match unsafe { line_memo::<7, 3>(p, memo) } {
            LineM::Slow => {
                ls = match unsafe { event_long(ctx, ix, p, ls, &mut dc, lm, &mut q, o, memo6) } {
                    Some(next) => next,
                    None => event_slow(ctx, ix, buf, ls, &mut dc, lm, &mut q, o),
                }
            }
            LineM::Bad(e) => ls += e + 1,
            LineM::Good(e, r, lo, hi) => {
                ls += e + 1;
                let mut f = MemoFields { p, pr: unsafe { p.add(r) }, lo, hi, i: 0, j: 0, _buf: std::marker::PhantomData };
                unsafe { event_line(ctx, ix, &mut dc, lm, &mut f, &mut q, o) };
            }
        }
    }
    for (base, n) in q.rest(lm) {
        flush_batch(base, n, lm, o);
    }
}

/// One parsed click waiting in the batch (one cache line).
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct ClSlot {
    /// The user as written; resolved to the person when the batch is flushed.
    user: Key,
    cid: Key,
    /// Hash of the user key.
    h: u64,
    /// Click time, ms from a week before the window start.
    ts: u32,
    camp: u32,
    linked: bool,
}

impl BatchSlot for ClSlot {
    #[inline(always)]
    fn hash(&self) -> u64 {
        self.h
    }
    #[inline(always)]
    fn set_linked(&mut self, linked: bool) {
        self.linked = linked;
    }
}

type ClQueue = BatchQueue<ClSlot>;

const CL_EMPTY: ClSlot = ClSlot { user: ZERO, cid: ZERO, h: 0, ts: 0, camp: 0, linked: false };

/// Parses the four fields of a click line, in order, into `slot`; false if
/// the click is to be ignored.
#[inline(always)]
unsafe fn click_fields<'a, S: Fields<'a>>(ctx: &Ctx, dc: &mut DateCache, lm: &LinkMap, src: &mut S, slot: *mut ClSlot, mapped: bool) -> bool {
    // click_id, user_id
    let f0 = src.lo();
    let f1 = src.lo();
    if f0.is_empty() || f1.is_empty() {
        return false;
    }
    // (a long click ID in a mapped file is kept by reference)
    let cid = if f0.len() > 15 && mapped && ref_ok(f0) { key_ref(f0) } else { key_of(f0) };
    (*slot).cid[0] = cid[0];
    (*slot).cid[1] = cid[1];
    // (only hashed if long: a person is named through its events or its links)
    let user = key_of_id(f1);
    (*slot).user[0] = user[0];
    (*slot).user[1] = user[1];
    let h = hk(&user);
    (*slot).h = h;
    lm.prefetch_filter(h);
    // campaign_id: a known campaign (the key of an empty field is the map's
    // empty marker, so it must not be looked up)
    let fc = src.hi();
    let camp = match dense::camp_number(fc) {
        // `c_` + number: two such IDs are equal exactly when the numbers are
        Some(n) if !ctx.camp_direct.is_empty() => {
            let c = *ctx.camp_direct.get_unchecked(n);
            if c == 0xFFFF {
                return false;
            }
            c as u32
        }
        _ => match ctx.campaigns.get(&key_of(fc)) {
            Some(c) if !fc.is_empty() => c,
            _ => return false,
        },
    };
    (*slot).camp = camp;
    // ts: from a week before the window to its end
    let lo = ctx.ws - WEEK;
    match parse_ts_dc(dc, src.hi()) {
        Some(t) if t >= lo && t < ctx.we => {
            (*slot).ts = (t - lo) as u32;
            true
        }
        _ => false,
    }
}

#[inline(always)]
fn flush_clicks(base: *const ClSlot, n: usize, lm: &LinkMap, clq: &mut Parts<RecCc, 2>, cesc: &mut [PVec<Key>]) {
    let pc_shift = 64 - pc_bits();
    for j in 0..n {
        let e = unsafe { &*base.add(j) };
        let mut person = e.user;
        let mut ph = e.h;
        if e.linked {
            person = lm.person(&person, ph);
            ph = hk(&person);
        }
        let q = (ph >> pc_shift) as usize;
        let esc = unsafe { cesc.get_unchecked_mut(q) };
        let rec = RecCc { p: compact(&person, esc), c: compact(&e.cid, esc), ts: e.ts, camp: e.camp };
        clq.push(q, rec);
    }
}

/// A line that did not fit the scanner's windows: reference scanner.
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn click_slow(
    ctx: &Ctx,
    buf: &[u8],
    ls: usize,
    dc: &mut DateCache,
    lm: &LinkMap,
    q: &mut ClQueue,
    clq: &mut Parts<RecCc, 2>,
    cesc: &mut [PVec<Key>],
    mapped: bool,
) -> usize {
    let (end, res) = slow_line::<4>(buf, ls);
    if let Some(f) = res {
        if unsafe { click_fields(ctx, dc, lm, &mut ArrFields { f: &f, i: 0 }, q.slot, mapped) } {
            if let Some((base, n)) = q.keep(lm) {
                flush_clicks(base, n, lm, clq, cesc);
            }
        }
    }
    end + 1
}

#[allow(clippy::too_many_arguments)]
fn parse_clicks(
    ctx: &Ctx,
    buf: &[u8],
    begin: usize,
    limit: usize,
    lm: &LinkMap,
    clq: &mut Parts<RecCc, 2>,
    cesc: &mut [PVec<Key>],
    memo: &mut ShapeMemo,
    memo3: &mut ShapeMemo,
    mapped: bool,
) {
    let n = buf.len();
    if begin >= n {
        return;
    }
    debug_assert!(buf[n - 1] == b'\n');
    let mut store = [[CL_EMPTY; EB]; 2];
    let mut q = ClQueue::new(&mut store);
    let mut dc = DateCache::new();
    let ptr = buf.as_ptr();
    let mut ls = begin;
    while ls < limit && ls < n {
        let p = unsafe { ptr.add(ls) };
        // (see scan_lean)
        prefetch(p.wrapping_add(INPUT_AHEAD));
        match unsafe { line_memo::<4, 2>(p, memo) } {
            LineM::Slow => {
                // (two long IDs: the first field taken off, as for events)
                let mut next = None;
                if let Some((f0, off)) = unsafe { first_field(p) } {
                    let pt = unsafe { p.add(off) };
                    match unsafe { line_memo_wide::<3, 1>(pt, memo3) } {
                        LineM::Slow => {}
                        LineM::Bad(e) => next = Some(ls + off + e + 1),
                        LineM::Good(e, r, lo, hi) => {
                            let mut f = Lead { first: Some(f0), rest: MemoFields { p: pt, pr: unsafe { pt.add(r) }, lo, hi, i: 0, j: 0, _buf: std::marker::PhantomData } };
                            if unsafe { click_fields(ctx, &mut dc, lm, &mut f, q.slot, mapped) } {
                                if let Some((base, n)) = q.keep(lm) {
                                    flush_clicks(base, n, lm, clq, cesc);
                                }
                            }
                            next = Some(ls + off + e + 1);
                        }
                    }
                }
                ls = match next {
                    Some(n) => n,
                    None => click_slow(ctx, buf, ls, &mut dc, lm, &mut q, clq, cesc, mapped),
                }
            }
            LineM::Bad(e) => ls += e + 1,
            LineM::Good(e, r, lo, hi) => {
                ls += e + 1;
                let mut f = MemoFields { p, pr: unsafe { p.add(r) }, lo, hi, i: 0, j: 0, _buf: std::marker::PhantomData };
                if unsafe { click_fields(ctx, &mut dc, lm, &mut f, q.slot, mapped) } {
                    if let Some((base, n)) = q.keep(lm) {
                        flush_clicks(base, n, lm, clq, cesc);
                    }
                }
            }
        }
    }
    for (base, n) in q.rest(lm) {
        flush_clicks(base, n, lm, clq, cesc);
    }
}

// ---------------------------------------------------------------------------
// Phase 3: sessions

struct P3 {
    uw: pq::UserWriter,
    name: Vec<u8>,
    agg: Vec<[u64; 4]>, // (day * (ncamp + 1) + camp) -> sessions, purchasing, revenue, assisted
    /// (day * (ncamp + 1) + camp) -> sessions. Every session bumps one of
    /// these counters at random; kept apart from `agg` (which only purchasing
    /// sessions touch) so that the table stays in the L2 cache.
    sess: Vec<u32>,
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
/// Adds events (ts << 32 | PURCHASE | index of the purchase) to the person
/// being put together in `st`.
#[inline(always)]
fn person_add(st: &mut P3, evs: &[u64]) {
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
}

/// Adds a run of events (ts | PURCHASE); its purchases are numbered from `pur0`.
#[inline(always)]
fn person_add_run(st: &mut P3, words: &[u32], pur0: u32) {
    let mut k = pur0;
    for &w in words {
        let t = w & TS_DEAD;
        let b = (t / GAP as u32) as usize;
        unsafe {
            *st.bits.get_unchecked_mut(b >> 6) |= 1 << (b & 63);
            let c = st.cell.get_unchecked_mut(b);
            c[0] = c[0].min(t);
            c[1] = c[1].max(t);
            c[2] += 1;
        }
        if w & PURCHASE != 0 {
            st.purs.push(((t as u64) << 32) | (PURCHASE | k) as u64);
            k += 1;
        }
    }
}

/// The sessions and days of the person whose events were added to `st`.
/// With `ordered`, the person's events are instead that one run of words
/// (ts | PURCHASE), in time order, and `st.purs` already holds its purchases.
/// Events are not sorted: two events less than 30 minutes apart are always in
/// one session, so events are summarized per 30-minute bucket (min, max,
/// count) and sessions are built from the buckets in time order: a session
/// breaks between two used buckets iff min(next) - max(previous) > 30 min.
/// `cls`: the person's clicks as ts << 32 | index into `camps`, sorted by ts,
/// ties by click id descending (so the last of equal ts has the smallest id).
/// Safety: every index it uses is one the pipeline made itself. `cls` holds
/// indices into `camps`, whose values are campaign numbers (below the length
/// of `st.cnt` and `st.pos`); the purchases in `st.purs` index `pur`, whose
/// categories are below the length of `st.catsum`; event times lie inside
/// the window, so their days are below the length of `st.pend` and their
/// buckets inside `st.bits` and `st.cell`.
unsafe fn person_finish(g: &Geo, st: &mut P3, person: Key, pur: &[(u32, u32)], cls: &[u64], camps: &[u32], ordered: Option<&[u32]>) {
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
                let n = *st.pend.get_unchecked_mut(d as usize);
                *st.pend.get_unchecked_mut(d as usize) = 0;
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
            while hi < cls.len() && ((*cls.get_unchecked(hi) >> 32) as i64) <= sc {
                let c = unsafe { *camps.get_unchecked(*cls.get_unchecked(hi) as u32 as usize) } as usize;
                if *st.cnt.get_unchecked_mut(c) == 0 {
                    flush_assists!();
                    *st.pos.get_unchecked_mut(c) = st.active.len() as u32;
                    st.active.push(c as u32);
                }
                *st.cnt.get_unchecked_mut(c) += 1;
                hi += 1;
            }
            while lo < hi && ((*cls.get_unchecked(lo) >> 32) as i64) < sc - WEEK {
                let c = unsafe { *camps.get_unchecked(*cls.get_unchecked(lo) as u32 as usize) } as usize;
                *st.cnt.get_unchecked_mut(c) -= 1;
                if *st.cnt.get_unchecked_mut(c) == 0 {
                    flush_assists!();
                    let p = *st.pos.get_unchecked_mut(c) as usize;
                    let last = st.active.pop().unwrap();
                    if p < st.active.len() {
                        *st.active.get_unchecked_mut(p) = last;
                        *st.pos.get_unchecked_mut(last as usize) = p as u32;
                    }
                }
                lo += 1;
            }
            let credit = if lo < hi { unsafe { *camps.get_unchecked(*cls.get_unchecked(hi - 1) as u32 as usize) } } else { g.organic } as usize;
            let cell = s_didx * (g.organic as usize + 1) + credit;
            unsafe { *st.sess.get_unchecked_mut(cell) += 1 };
            if s_purch {
                let a = unsafe { st.agg.get_unchecked_mut(cell) };
                a[1] += 1;
                a[2] += s_rev;
            }
            if !st.active.is_empty() {
                if *st.pend.get_unchecked_mut(s_didx) == 0 {
                    st.pend_days.push(s_didx as u32);
                }
                *st.pend.get_unchecked_mut(s_didx) += 1;
            }
        }};
    }
    macro_rules! flush_day {
        () => {{
            let mut top = -1i32;
            let mut best = 0u64;
            for &c in &st.touched {
                let v = *st.catsum.get_unchecked_mut(c as usize);
                if v > best || (v == best && (c as i32) < top) {
                    best = v;
                    top = c as i32;
                }
                *st.catsum.get_unchecked_mut(c as usize) = 0;
            }
            st.touched.clear();
            st.uw.row((cur_day - g.day0) as usize, d_sessions, d_events, d_rev, top);
        }};
    }

    let mut pi = 0usize;
    // A stretch of events from `t` to `last` (less than 30 minutes): it goes
    // on the open session unless it starts more than 30 minutes after it.
    macro_rules! stretch {
        ($t:expr, $last:expr, $n:expr) => {{
            let (t, last): (i64, i64) = ($t, $last);
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
            d_events += $n;
            // the purchases of this stretch
            while pi < st.purs.len() && (*st.purs.get_unchecked(pi) >> 32) as i64 <= last {
                let e = *st.purs.get_unchecked(pi);
                pi += 1;
                let (p, c) = unsafe { *pur.get_unchecked((e as u32 & !PURCHASE) as usize) };
                let p = p as u64;
                s_rev += p;
                s_purch = true;
                d_rev += p;
                let c = c as usize;
                if *st.catsum.get_unchecked_mut(c) == 0 {
                    st.touched.push(c as u32);
                }
                *st.catsum.get_unchecked_mut(c) += p;
            }
        }};
    }
    if let Some(words) = ordered {
        // already in time order: every event is its own stretch
        for &w in words {
            let t = (w & TS_DEAD) as i64;
            stretch!(t, t, 1);
        }
    } else {
        if st.purs.len() > 1 {
            st.purs.sort_unstable();
        }
        for wi in 0..st.bits.len() {
            let mut word = *st.bits.get_unchecked_mut(wi);
            if word == 0 {
                continue;
            }
            *st.bits.get_unchecked_mut(wi) = 0;
            while word != 0 {
                let b = wi * 64 + word.trailing_zeros() as usize;
                word &= word - 1;
                let c = std::mem::replace(unsafe { st.cell.get_unchecked_mut(b) }, [u32::MAX, 0, 0, 0]);
                stretch!(c[0] as i64, c[1] as i64, c[2]);
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
        *st.cnt.get_unchecked_mut(c as usize) = 0;
    }
    st.active.clear();
}

/// Per-partition local person table (generation-stamped, never cleared).
/// Persons of one partition, numbered in order of first appearance. Looked
/// up by the 12-byte compact key as it sits in the records (no expansion, one
/// multiplication); the few keys that do not fit the compact form go through
/// a map of full keys.
/// A cheap hasher for keys (they are hashes or packed bytes already).
#[derive(Default, Clone, Copy)]
struct KeyHasher(u64);

impl std::hash::Hasher for KeyHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            self.write_u64(u64::from_le_bytes(w));
        }
    }
    #[inline(always)]
    fn write_u64(&mut self, v: u64) {
        self.0 = fm(self.0 ^ v, 0x9E37_79B9_7F4A_7C15);
    }
    #[inline(always)]
    fn write_usize(&mut self, v: usize) {
        self.write_u64(v as u64);
    }
}

type KeyBuild = std::hash::BuildHasherDefault<KeyHasher>;

struct LocalIds {
    slots: Vec<LSlot>,
    mask: usize,
    persons: Vec<Key>,
    long: std::collections::HashMap<Key, u32, KeyBuild>,
}

/// `a`, `b`: the compact key; `idp`: person id + 1 (0 = empty).
#[derive(Clone, Copy)]
#[repr(C)]
struct LSlot {
    a: u64,
    b: u32,
    idp: u32,
}

const LEMPTY: LSlot = LSlot { a: 0, b: 0, idp: 0 };

/// The two words of a compact key that is not an escape.
#[inline(always)]
fn ck_words(c: &Ck) -> (u64, u32) {
    (c[0] as u64 | (c[1] as u64) << 32, c[2])
}

impl LocalIds {
    fn new() -> LocalIds {
        LocalIds { slots: Vec::new(), mask: 0, persons: Vec::new(), long: std::collections::HashMap::default() }
    }
    /// Starts a new partition expecting about `n` persons.
    fn reset(&mut self, n: usize) {
        let size = (n * 2).next_power_of_two().max(256);
        if size > self.slots.len() {
            self.slots = vec![LEMPTY; size];
        } else {
            self.slots[..size].fill(LEMPTY);
        }
        self.mask = size - 1;
        self.persons.clear();
        self.long.clear();
    }
    #[inline(always)]
    fn home(&self, a: u64, b: u32) -> usize {
        // (one multiplication: the table is small and the full key is compared)
        ((a ^ b as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) as usize & self.mask
    }
    #[cold]
    fn grow(&mut self) {
        let size = (self.mask + 1) * 2;
        if size > self.slots.len() {
            self.slots = vec![LEMPTY; size];
        } else {
            self.slots[..size].fill(LEMPTY);
        }
        self.mask = size - 1;
        for (id, k) in self.persons.iter().enumerate() {
            if let Some(c) = compact_plain(k) {
                let (a, b) = ck_words(&c);
                let mut i = self.home(a, b);
                while self.slots[i].idp != 0 {
                    i = (i + 1) & self.mask;
                }
                self.slots[i] = LSlot { a, b, idp: id as u32 + 1 };
            }
        }
    }
    #[inline]
    fn intern_long(&mut self, k: Key) -> u32 {
        if let Some(&id) = self.long.get(&k) {
            return id;
        }
        let id = self.persons.len() as u32;
        self.long.insert(k, id);
        self.persons.push(k);
        id
    }
    /// The person id of the compact key `c` (escapes resolved through `esc`).
    #[inline(always)]
    fn intern(&mut self, c: &Ck, esc: &[Key]) -> u32 {
        if c[2] & 0xFF == ESCAPE {
            return self.intern_long(expand(c, esc));
        }
        let (a, b) = ck_words(c);
        let mut i = self.home(a, b);
        loop {
            let s = unsafe { self.slots.get_unchecked_mut(i) };
            if s.idp == 0 {
                let id = self.persons.len() as u32;
                *s = LSlot { a, b, idp: id + 1 };
                self.persons.push(expand(c, esc));
                if self.persons.len() * 2 > self.mask + 1 {
                    self.grow();
                }
                return id;
            }
            if s.a == a && s.b == b {
                return s.idp - 1;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, c: &Ck, esc: &[Key]) -> Option<u32> {
        if c[2] & 0xFF == ESCAPE {
            return self.long.get(&expand(c, esc)).copied();
        }
        let (a, b) = ck_words(c);
        let mut i = self.home(a, b);
        loop {
            let s = unsafe { self.slots.get_unchecked(i) };
            if s.idp == 0 {
                return None;
            }
            if s.a == a && s.b == b {
                return Some(s.idp - 1);
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

/// A worker's session state (see `P3`).
fn p3_new(out: &Path, w: usize, ctx: &Ctx, day0: i64, ndays: usize, nbuckets: usize) -> P3 {
    let ncamp = ctx.camp_ids.len();
    P3 {
        uw: pq::UserWriter::new(&out.join("user_daily"), w, day0 as i32, ndays, &ctx.cat_names),
        name: Vec::new(),
        agg: vec![[0u64; 4]; (ncamp + 1) * ndays],
        sess: vec![0; (ncamp + 1) * ndays],
        assist: vec![0; (ncamp + 1) * ndays],
        cnt: vec![0; ncamp + 1],
        pos: vec![0; ncamp + 1],
        active: Vec::new(),
        pend: vec![0; ndays],
        pend_days: Vec::new(),
        catsum: vec![0; ctx.cat_names.len()],
        touched: Vec::new(),
        cell: vec![[u32::MAX, 0, 0, 0]; nbuckets],
        bits: vec![0; nbuckets / 64],
        purs: Vec::new(),
    }
}

/// The worker's campaign counters, complete (sessions and assists folded in).
fn p3_agg(mut st: P3) -> Vec<[u64; 4]> {
    for (i, a) in st.agg.iter_mut().enumerate() {
        a[0] = st.sess[i] as u64;
        a[3] = st.assist[i] as u64;
    }
    st.agg
}

/// Sums the workers' counters and writes the campaign_daily table.
fn write_campaign_daily(out: &Path, ctx: &Ctx, aggs: &[Vec<[u64; 4]>], day0: i64, ndays: usize) {
    let ncamp = ctx.camp_ids.len();
    let mut total = vec![[0u64; 4]; (ncamp + 1) * ndays];
    for a in aggs {
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
    {
        // a diagnostic build must never be scored: one extra row
        n += 1;
        cols[0].push_str(b"diag");
        cols[1].push_str(b"diag");
        cols[2].push_i32(day0 as i32);
        for k in 0..4 {
            cols[3 + k].push_i64(k as i64);
        }
    }
    pq::write_file(&out.join("campaign_daily").join("part-000.parquet"), &cols, n).expect("write campaign_daily");
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
    diag::run(&data, &out, nthreads);
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
        let mut camp_direct = if camp_ids.len() < 0xFFFF { vec![0xFFFFu16; dense::CAMP_DIRECT] } else { Vec::new() };
        if !camp_direct.is_empty() {
            for (i, id) in camp_ids.iter().enumerate() {
                let mut padded = id.clone();
                padded.resize(id.len() + 16, 0);
                if let Some(n) = dense::camp_number(&padded[..id.len()]) {
                    if camp_direct[n] == 0xFFFF {
                        camp_direct[n] = i as u16;
                    }
                }
            }
        }
        Ctx {
            ws,
            we,
            ingest_base_s: ws.div_euclid(1000) - (1 << 23),
            products,
            unknown_cat,
            cat_names,
            campaigns,
            camp_direct,
            camp_ids,
            camp_channels,
        }
    };
    let ctx_cell: OnceLock<Ctx> = OnceLock::new();
    lap("small inputs");

    // The input files are listed and mapped. (The products and campaigns are
    // read later, on a thread of their own next to the link phases: nothing
    // here waits for them.) The dense engine, when it is switched on, takes
    // the data if its IDs have the old generated shape; anything it declines
    // goes through the general pipeline below.
    let (files, chunks, dense_done) = {
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
        // long IDs get a table with a slot for about every fortieth line (the
        // generator writes one ID in eight long; only user IDs are remembered)
        let lines = files.iter().map(|f| f.len).sum::<u64>() / 70;
        LONG_SLOTS.store(((lines / 40) as usize).max(1 << 16).next_power_of_two(), Ordering::Relaxed);
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
        let done = match dense::detect(&files, nthreads, verbose) {
            Some(keys) => dense::run(&keys, ctx_cell.get_or_init(&build_ctx), &files, &chunks, &out, nthreads, verbose, t0),
            None => false,
        };
        (files, chunks, done)
    };
    if dense_done {
        lap("done");
        if let Some(p) = &prof_path {
            prof::dump(p);
        }
        std::process::exit(0);
    }
    // estimate line counts to pre-size the partition buffers
    let bytes_of = |k: Kind| files.iter().filter(|f| f.kind == k).map(|f| f.len).sum::<u64>();
    // (bytes per line from the head of the kind's biggest file)
    let line_len = |k: Kind, usual: u64| -> u64 {
        match files.iter().filter(|f| f.kind == k && !f.map.is_null()).max_by_key(|f| f.len) {
            Some(f) => {
                let n = (f.len as usize).min(64 << 10);
                let lines = unsafe { std::slice::from_raw_parts(f.map, n) }.iter().filter(|&&b| b == b'\n').count() as u64;
                if lines >= 64 {
                    (n as u64 / lines).max(8)
                } else {
                    usual
                }
            }
            None => usual,
        }
    };
    let est_events = (bytes_of(Kind::Event) / line_len(Kind::Event, 70)) as usize * 33 / 32;
    let est_clicks = (bytes_of(Kind::Click) / line_len(Kind::Click, 40)) as usize * 33 / 32;
    // An eighth to spare per buffer (the line counts are estimated from a
    // sample, and a partition's share of a thread's lines varies by a few
    // percent): what is reserved is zeroed a huge page at a time whether it is
    // used or not, and a tighter reserve was 3% faster.
    let resk: usize = std::env::var("SS_RESK").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    let per = |n: usize, parts: usize| n / parts / nthreads + n / parts / nthreads / resk + 64;
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
    // --- sequence numbers of generated event ids (see IxCfg): the key is
    // searched with the ids of the first lines of the biggest events file
    let ix = {
        let mut cfg = IxCfg { on: false, key: 0, shift: 0, mask: 0, parts: 1, imax: 0 };
        let biggest = files.iter().filter(|f| f.kind == Kind::Event && !f.map.is_null()).max_by_key(|f| f.len);
        // (off unless asked for: the rules say IDs are opaque strings)
        if let (Some(f), true) = (biggest, std::env::var_os("SS_SEQ").is_some()) {
            let head = unsafe { std::slice::from_raw_parts(f.map, (f.len as usize).min(96 << 10)) };
            if let Some(last) = head.iter().rposition(|&b| b == b'\n') {
                let text = pad_buffer(head[..last + 1].to_vec());
                let begin = find_byte(&text, 0, b'\n') + 1;
                let mut sample: Vec<u64> = Vec::new();
                scan_lines::<7, _>(&text, begin, text.len(), |fl| {
                    if sample.len() < 48 {
                        if let Some(v) = id_number(b'e', fl[0]) {
                            sample.push(v);
                        }
                    }
                });
                let bound = (est_events as u64).max(1 << 12);
                if let Some(key) = find_seq_key(&sample, bound, nthreads) {
                    // Partitions of 2^14 numbers while that makes at most about
                    // 4400 of them. Measured on the scorer at 20M events: 2^12
                    // to 2^14 numbers per partition are equally fast, 2^15
                    // costs 1% and 2^16 3% (in the sequence phase), and phase 1
                    // is as fast with 5800 partitions as with 360.
                    let mut shift = 14;
                    while (bound >> shift) > 4400 && shift < 16 {
                        shift += 1;
                    }
                    let parts = (((bound >> shift) + 1) as usize).min(8192);
                    cfg = IxCfg { on: true, key, shift, mask: (1 << shift) - 1, parts, imax: (parts as u64) << shift };
                }
            }
        }
        cfg
    };
    if verbose {
        eprintln!("  sequence numbers: on={} key={:x} parts={} x {}", ix.on, ix.key, ix.parts, 1u64 << ix.shift);
    }
    // a partition of numbers is dense: 2^shift events, their few extra copies
    // and the purchases' second records, spread over the threads
    let res_ix = if ix.on { ((1usize << ix.shift) * 9 / 8 / nthreads) * 5 / 4 + 64 } else { 4 };
    // with sequence numbers the general path is for the odd rows only
    let (res_dd, res_pay1) = if ix.on { (res_dd / 8 + 64, res_pay / 8 + 64) } else { (res_dd, res_pay) };
    lap("chunks");
    let presize_on = std::env::var_os("SS_NO_PRESIZE").is_none();

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
        let mut memo_ev = ShapeMemo::new();
        let mut memo_ev6 = ShapeMemo::new();
        let mut memo_cl3 = ShapeMemo::new();
        let mut memo_cl = ShapeMemo::new();
        let mut o = P1Work {
            ix: Parts::new(ix.parts, res_ix),
            ixodd: (0..ix.parts).map(|_| PVec::with_capacity(16)).collect(),
            dd: Parts::new(pa, res_dd),
            pay: Parts::new(pc, res_pay1),
            pur: (0..pc).map(|_| PVec::with_capacity(res_pay1 / 24 + 16)).collect(),
            // (keys that do not fit the compact form leave a full key here:
            // a person ID of 12 to 15 bytes, which the generator does not
            // write, and a long click ID, one click in eight)
            pesc: (0..pc).map(|_| PVec::with_capacity(res_pay1 / 64 + 16)).collect(),
            clq: Parts::new(pc, res_cl),
            cesc: (0..pc).map(|_| PVec::with_capacity(res_cl / 7 + 16)).collect(),
        };
        // (the buffers of a Parts are allocated one after the other)
        let ix_region = {
            let first = o.ix.out[0].ptr as usize;
            let last = o.ix.out[ix.parts - 1].ptr as usize + o.ix.out[ix.parts - 1].cap * std::mem::size_of::<RecX>();
            (first, last)
        };
        // the dedup buffers were allocated first, contiguously; they are dead
        // after phase 2a and phase 3 reuses that memory
        let dd_region = {
            let first = o.dd.out[0].ptr as usize;
            let last = o.dd.out[pa - 1].ptr as usize + o.dd.out[pa - 1].cap * std::mem::size_of::<RecD>();
            (first, last)
        };

        // (see Parts::presize)
        let (mut ev_seen, mut cl_seen) = (false, false);
        let ahead = |kind: Kind, c: &Chunk| bytes_of(kind) as f64 / nthreads as f64 / (c.end - c.start).max(1) as f64;
        loop {
            let i = counters[4].fetch_add(1, Ordering::Relaxed);
            if i >= data_chunks.len() {
                break;
            }
            let c = &chunks[data_chunks[i]];
            let (bytes, b, l) = chunk_bytes(&files[c.file], c, &mut buf);
            match c.kind {
                Kind::Event => parse_events(ctx, &ix, bytes, b, l, lm, &mut o, &mut memo_ev, &mut memo_ev6),
                // (the bytes are the file's mapping itself, or a copy that goes away)
                Kind::Click => parse_clicks(ctx, bytes, b, l, lm, &mut o.clq, &mut o.cesc, &mut memo_cl, &mut memo_cl3, !files[c.file].map.is_null() && bytes.as_ptr() == files[c.file].map),
                Kind::Link => unreachable!(),
            }
            if c.kind == Kind::Event && !ev_seen {
                ev_seen = true;
                let f = ahead(Kind::Event, c);
                if f > 1.5 && presize_on {
                    o.pay.presize(f);
                    presize_lists(&mut o.pesc, f);
                    presize_lists(&mut o.pur, f);
                }
            } else if c.kind == Kind::Click && !cl_seen {
                cl_seen = true;
                let f = ahead(Kind::Click, c);
                if f > 1.5 && presize_on {
                    o.clq.presize(f);
                    presize_lists(&mut o.cesc, f);
                }
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
            ix_region,
            ix: o.ix.finish(),
            ixodd: o.ixodd,
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
    // The payload of a losing copy sits anywhere in the phase-1 buffers (a
    // cache miss): its address is prefetched and the write happens KQ kills
    // later.
    const KQ: usize = 16;
    let kill_target = |w: usize, b: u64| -> *mut RecP {
        if b & HAS_PAY == 0 {
            return std::ptr::null_mut();
        }
        let q = ((b >> IDX_BITS) as usize) & (pc - 1);
        let i = (b as usize) & ((1 << IDX_BITS) - 1);
        unsafe { p1[w].pay[q].ptr.add(i) }
    };
    let kill_now = |r: *mut RecP| {
        if !r.is_null() {
            unsafe { (*r).t = ((*r).t & PURCHASE) | TS_DEAD };
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
        let mut pending = [std::ptr::null_mut::<RecP>(); KQ];
        let mut npending = 0usize;
        let mut kill = |w: usize, b: u64| {
            let r = kill_target(w, b);
            if !r.is_null() {
                prefetch_w(r);
                kill_now(std::mem::replace(&mut pending[npending % KQ], r));
                npending += 1;
            }
        };
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= pa {
                let j = if ix.on { lm_slices } else { next_lm.fetch_add(1, Ordering::Relaxed) };
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
        for r in pending {
            kill_now(r);
        }
        release_vec(&table);
    });
    lap("phase 2a dedup");

    // --- phase 2b: events with a sequence number. Per partition of numbers:
    // a table indexed by the number keeps the copy with the smallest ingest
    // time; then, in number order, the valid ones go to their person's
    // partition. (Numbers follow users, so the link map is read user by user.)
    struct PbOut {
        /// Per person partition, runs of events of one person: the number
        /// of events, the person (compact key), the number of purchases, then
        /// one word per event (ts | PURCHASE).
        words: Vec<PVec<u32>>,
        pur: Vec<PVec<(u32, u32)>>,
        pesc: Vec<PVec<Key>>,
    }
    const RUN_MAX: usize = 1 << 16;
    let next_ix = AtomicUsize::new(0);
    let pb_barrier = std::sync::Barrier::new(nthreads);
    let pb: Vec<PbOut> = if ix.on {
        run_workers(nthreads, |_| {
            // (a word per event and five per run: `res_pay` words leave a third to spare)
            let mut words: Parts<u32, 16> = Parts::new(pc, res_pay);
            let mut pur: Vec<PVec<(u32, u32)>> = (0..pc).map(|_| PVec::with_capacity(res_pay / 24 + 16)).collect();
            let mut pesc: Vec<PVec<Key>> = (0..pc).map(|_| PVec::with_capacity(16)).collect();
            let mut table = vec![u64::MAX; 1usize << ix.shift];
            let pc_shift = 64 - pc_bits();
            // users resolved lately: (number, person, person's hash), by the number's low bits
            const UC: usize = 256;
            let mut cache = [(u64::MAX, ZERO, 0u64); UC];
            // the run being collected: its person and partition, its events
            let mut run: Vec<u32> = Vec::with_capacity(RUN_MAX);
            let (mut run_person, mut run_q, mut run_npur) = (ZERO, 0usize, 0u32);
            // the user field of the last event (number and flag)
            let mut last_user = u64::MAX;
            macro_rules! flush_run {
                () => {
                    if !run.is_empty() {
                        let ck = compact(&run_person, unsafe { pesc.get_unchecked_mut(run_q) });
                        words.extend(run_q, &[run.len() as u32, ck[0], ck[1], ck[2], run_npur]);
                        words.extend(run_q, &run);
                        run.clear();
                        run_npur = 0;
                    }
                };
            }
            loop {
                let i = next_ix.fetch_add(1, Ordering::Relaxed);
                if i >= ix.parts {
                    break;
                }
                // entry: ingest << 40 | worker << 32 | position; the smallest wins
                let mut any = false;
                let mut bases = [std::ptr::null::<RecX>(); 32];
                for (w, o) in p1.iter().enumerate() {
                    let recs = o.ix[i].as_slice();
                    bases[w] = recs.as_ptr();
                    let tag = (w as u64) << 32;
                    let mut pos = 0;
                    while pos < recs.len() {
                        let r = unsafe { recs.get_unchecked(pos) };
                        // (ingest sits at bits 16..40 of a)
                        let cand = ((r.a << 24) & 0xFFFF_FF00_0000_0000) | tag | pos as u64;
                        let e = unsafe { table.get_unchecked_mut(r.slot()) };
                        if cand < *e {
                            *e = cand;
                        }
                        pos += 1 + (r.b & X_PURCHASE != 0) as usize;
                        any = true;
                    }
                }
                if !any {
                    continue;
                }
                for e in table.iter_mut() {
                    let c = std::mem::replace(e, u64::MAX);
                    if c == u64::MAX {
                        continue;
                    }
                    let (w, pos) = ((c >> 32) as usize & 0xFF, c as u32 as usize);
                    let rp = unsafe { bases.get_unchecked(w).add(pos) };
                    let r = unsafe { *rp };
                    if r.b & X_VALID == 0 {
                        continue;
                    }
                    let user = r.user();
                    let field = user | (r.b & X_ODD);
                    if field != last_user || r.b & X_ODD != 0 {
                        let (person, ph) = if r.b & X_ODD != 0 {
                            let key = p1[w].ixodd[i].as_slice()[user as usize];
                            let h = hk(&key);
                            if linkmap.maybe_linked(h) {
                                let p = linkmap.person(&key, h);
                                (p, hk(&p))
                            } else {
                                (key, h)
                            }
                        } else {
                            // (the low bits of a code are its first byte's)
                            let ce = unsafe { cache.get_unchecked_mut((user ^ (user >> 23)) as usize % UC) };
                            if ce.0 != user {
                                let key = user_code_key(user);
                                let h = hk(&key);
                                *ce = if linkmap.maybe_linked(h) {
                                    let p = linkmap.person(&key, h);
                                    (user, p, hk(&p))
                                } else {
                                    (user, key, h)
                                };
                            }
                            (ce.1, ce.2)
                        };
                        last_user = field;
                        if person != run_person {
                            flush_run!();
                            run_person = person;
                            run_q = (ph >> pc_shift) as usize;
                        }
                    }
                    if run.len() == RUN_MAX {
                        flush_run!();
                    }
                    let purchase = (r.b & X_PURCHASE != 0) as u32;
                    if purchase != 0 {
                        let x = unsafe { *rp.add(1) };
                        unsafe { pur.get_unchecked_mut(run_q) }.push((x.a as u32, (x.a >> 32) as u32));
                        run_npur += 1;
                    }
                    // (room for RUN_MAX words was reserved)
                    unsafe {
                        let n = run.len();
                        run.as_mut_ptr().add(n).write(r.ts() | (purchase << 31));
                        run.set_len(n + 1);
                    }
                }
                // (a person's events may go on in the next partition, which
                // another thread may have: that is a second run)
                flush_run!();
                last_user = u64::MAX;
            }
            // nobody reads the link map any more: free it, a slice each
            pb_barrier.wait();
            loop {
                let j = next_lm.fetch_add(1, Ordering::Relaxed);
                if j >= lm_slices {
                    break;
                }
                let n = (1usize << 16).min(linkmap.size() - (j << 16));
                release(unsafe { linkmap.slots.add(j << 16) } as *const u8, n * std::mem::size_of::<Slot>());
            }
            PbOut { words: words.finish(), pur, pesc }
        })
    } else {
        Vec::new()
    };
    lap("phase 2b sequence numbers");
    // phase 3 reuses memory that is dead now
    for (w, o) in p1.iter().enumerate() {
        if ix.on {
            recycle(w, o.ix_region.0, o.ix_region.1);
        } else {
            recycle(w, o.dd_region.0, o.dd_region.1);
        }
    }
    /// A run of phase 2b in a person partition: the local person, where its
    /// event words and its purchases are.
    #[derive(Clone, Copy, Default)]
    struct RunRef {
        pid: u32,
        src: u32,
        off: u32,
        len: u32,
        pur0: u32,
        npur: u32,
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
        .map(|q| (p1.iter().map(|o| o.pay[q].len()).sum::<usize>() + pb.iter().map(|o| o.words[q].len()).sum::<usize>(), q))
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
            sess: vec![0; (ncamp + 1) * ndays],
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
        let mut runs: Vec<RunRef> = Vec::new();
        let mut rsorted: Vec<RunRef> = Vec::new();
        let mut rstart: Vec<u32> = Vec::new();
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
            // (the scratch vectors have room for every record of the biggest
            // partition, so nothing is checked per event)
            let (ep, pp, lp) = (evs.as_mut_ptr(), pids.as_mut_ptr(), lpur.as_mut_ptr());
            let (mut nev, mut npu) = (0usize, 0usize);
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
                    let id = lid.intern(&e.p, esc);
                    let x = if purchase != 0 {
                        unsafe { *lp.add(npu) = *side.get_unchecked(pk) };
                        pk += 1;
                        npu += 1;
                        PURCHASE | (npu - 1) as u32
                    } else {
                        0
                    };
                    unsafe {
                        *pp.add(nev) = id;
                        *ep.add(nev) = ((ts as u64) << 32) | x as u64;
                    }
                    nev += 1;
                }
            }
            debug_assert!(nev <= evs.capacity() && npu <= lpur.capacity());
            unsafe {
                evs.set_len(nev);
                pids.set_len(nev);
                lpur.set_len(npu);
            }
            // the runs of the sequence-number path: one person each
            runs.clear();
            for (src, o) in pb.iter().enumerate() {
                let words = o.words[q].as_slice();
                let esc = o.pesc[q].as_slice();
                let (mut at, mut pk) = (0usize, 0u32);
                while at < words.len() {
                    let (len, npur) = (words[at], words[at + 4]);
                    let pid = lid.intern(&[words[at + 1], words[at + 2], words[at + 3]], esc);
                    runs.push(RunRef { pid, src: src as u32, off: at as u32 + 5, len, pur0: pk, npur });
                    pk += npur;
                    at += 5 + len as usize;
                }
            }
            if evs.is_empty() && runs.is_empty() {
                continue;
            }
            tick!(0);
            let np = lid.persons.len();
            // the runs by person (none unless the sequence-number path ran)
            let has_runs = !runs.is_empty();
            if has_runs {
                rstart.clear();
                rstart.resize(np + 1, 0);
                for r in runs.iter() {
                    rstart[r.pid as usize + 1] += 1;
                }
                for k in 0..np {
                    rstart[k + 1] += rstart[k];
                }
                rsorted.clear();
                rsorted.resize(runs.len(), RunRef::default());
                fill.clear();
                fill.extend_from_slice(&rstart[..np]);
                for r in runs.iter() {
                    let f = &mut fill[r.pid as usize];
                    rsorted[*f as usize] = *r;
                    *f += 1;
                }
            }
            // counting sort by pid (every pid is below np)
            start.clear();
            start.resize(np + 1, 0);
            for &p in pids.iter() {
                unsafe { *start.get_unchecked_mut(p as usize + 1) += 1 };
            }
            let mut acc = 0u32;
            for s in start.iter_mut() {
                acc += *s;
                *s = acc;
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
            // (room for every click record of the biggest partition)
            let (clp, cap, cip, cpp) = (cls.as_mut_ptr(), ccamp.as_mut_ptr(), ccid.as_mut_ptr(), cpid.as_mut_ptr());
            let mut ncl = 0usize;
            for o in p1.iter() {
                let esc = o.cesc[q].as_slice();
                for c in o.clq[q].as_slice() {
                    if let Some(id) = lid.get(&c.p, esc) {
                        unsafe {
                            *clp.add(ncl) = ((c.ts as u64) << 32) | ncl as u64;
                            *cap.add(ncl) = c.camp;
                            *cip.add(ncl) = expand(&c.c, esc);
                            *cpp.add(ncl) = id;
                        }
                        ncl += 1;
                    }
                }
            }
            debug_assert!(ncl <= cls.capacity());
            unsafe {
                cls.set_len(ncl);
                ccamp.set_len(ncl);
                ccid.set_len(ncl);
                cpid.set_len(ncl);
            }
            cstart.clear();
            cstart.resize(np + 1, 0);
            for &p in cpid.iter() {
                unsafe { *cstart.get_unchecked_mut(p as usize + 1) += 1 };
            }
            let mut acc = 0u32;
            for s in cstart.iter_mut() {
                acc += *s;
                *s = acc;
            }
            csorted.clear();
            csorted.reserve(cls.len());
            unsafe { csorted.set_len(cls.len()) };
            fill.clear();
            fill.extend_from_slice(&cstart[..np]);
            for (c, &p) in cls.iter().zip(cpid.iter()) {
                let f = unsafe { fill.get_unchecked_mut(p as usize) };
                unsafe { *csorted.get_unchecked_mut(*f as usize) = *c };
                *f += 1;
            }
            tick!(2);
            tick!(5);
            for p in 0..np {
                // (cstart: np + 1 rising offsets into csorted)
                let cl = unsafe { csorted.get_unchecked_mut(*cstart.get_unchecked(p) as usize..*cstart.get_unchecked(p + 1) as usize) };
                if cl.len() > 1 {
                    cl.sort_unstable();
                    // equal timestamps: order by click id, descending. Only
                    // the campaign of the last of them is ever looked at, so
                    // clicks of one campaign (the copies of a click, above
                    // all) are left as they are: comparing their IDs would
                    // read the click files again, long after their pages
                    // were given back.
                    let mut a = 0;
                    while a < cl.len() {
                        let mut b = a + 1;
                        while b < cl.len() && cl[b] >> 32 == cl[a] >> 32 {
                            b += 1;
                        }
                        if b - a > 1 {
                            let c0 = ccamp[cl[a] as u32 as usize];
                            if cl[a + 1..b].iter().any(|x| ccamp[*x as u32 as usize] != c0) {
                                cl[a..b].sort_unstable_by(|x, y| cmp_key(&ccid[*y as u32 as usize], &ccid[*x as u32 as usize]));
                            }
                        }
                        a = b;
                    }
                }
            }
            tick!(6);
            let loose_pur = lpur.len();
            for p in 0..np {
                let loose = unsafe { sorted.get_unchecked(*start.get_unchecked(p) as usize..*start.get_unchecked(p + 1) as usize) };
                let prs: &[RunRef] = if has_runs { &rsorted[rstart[p] as usize..rstart[p + 1] as usize] } else { &[] };
                let cl = unsafe { csorted.get_unchecked(*cstart.get_unchecked(p) as usize..*cstart.get_unchecked(p + 1) as usize) };
                let mut ordered = None;
                if loose.is_empty() && prs.len() == 1 {
                    // the usual case: one run. If it is in time order (it is,
                    // from the generator) its events need no sorting at all.
                    let r = &prs[0];
                    let o = &pb[r.src as usize];
                    let words = &o.words[q].as_slice()[r.off as usize..(r.off + r.len) as usize];
                    // (lpur has room for every event of the partition)
                    lpur.extend_from_slice(&o.pur[q].as_slice()[r.pur0 as usize..(r.pur0 + r.npur) as usize]);
                    let (mut in_order, mut prev, mut k) = (true, 0u32, loose_pur as u32);
                    for &w in words {
                        let t = w & TS_DEAD;
                        in_order &= t >= prev;
                        prev = t;
                        if w & PURCHASE != 0 {
                            st.purs.push(((t as u64) << 32) | (PURCHASE | k) as u64);
                            k += 1;
                        }
                    }
                    if in_order {
                        ordered = Some(words);
                    } else {
                        st.purs.clear();
                        person_add_run(&mut st, words, loose_pur as u32);
                    }
                } else {
                    person_add(&mut st, loose);
                    for r in prs {
                        let o = &pb[r.src as usize];
                        let words = &o.words[q].as_slice()[r.off as usize..(r.off + r.len) as usize];
                        let pur0 = lpur.len() as u32;
                        lpur.extend_from_slice(&o.pur[q].as_slice()[r.pur0 as usize..(r.pur0 + r.npur) as usize]);
                        person_add_run(&mut st, words, pur0);
                    }
                }
                unsafe { person_finish(&geo, &mut st, lid.persons[p], &lpur, cl, &ccamp, ordered) };
                unsafe { lpur.set_len(loose_pur) };
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
                st.agg[d * (ncamp + 1) + c][0] = st.sess[d * (ncamp + 1) + c] as u64;
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

    write_campaign_daily(&out, ctx, &aggs, day0, ndays);
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
