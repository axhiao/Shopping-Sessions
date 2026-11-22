//! The dense engine: the whole pipeline for inputs whose IDs have the shape
//! the published generator writes.
//!
//! An event ID is `e` plus base 36 of scramble(n), a user ID `u` plus base 36
//! of scramble(user * 512 + alias): n numbers the events, `user` the users
//! (each with a few alias IDs), and scramble is a fixed bijection on 40-bit
//! numbers with one key per kind of ID (see "Sequence numbers behind generated
//! IDs" in main.rs). The two keys are searched with a sample of the input.
//! With them:
//!
//! - an event copy is stored straight into a table indexed by its number: the
//!   copy with the smallest `ingest_ts` stays (dedup is an array store);
//! - a user ID gives (user, alias) by arithmetic, so the links between the
//!   aliases of one user are a 64-bit word per user instead of a hash table;
//! - the generator numbers events user by user in time order, so reading the
//!   table in order meets every user's events together, already sorted.
//!
//! Nothing is taken on trust. Decoding is exact whatever the keys are (a
//! bijection), so copies of an event always meet and different IDs never do.
//! Every ID that does not decode, every user whose links leave its own first
//! 16 aliases (shared devices, long chains, upper-cased IDs) and every row
//! that depends on such an ID is handled by text, through a small general
//! union-find and a sort ("complex" below). Time order inside a user is
//! checked, never assumed. If the table is not laid out user by user the
//! engine gives up before anything is kept, and the general pipeline in
//! main.rs runs instead.

use super::*;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;

// A cell of the event table: two words, all zero while empty.
// w0: PRESENT | VALID | PURCH | ODD | LOCK | ingest seconds (28 bits) << 31 |
//     ts (31 bits, ms from the window start). LOCK is set only while a
//     thread is writing the cell.
// w1: the user. A purchase: price << 40 | category << 32 | user << 4 | alias.
//     Any other event: the bytes of the user ID after its `u` (at most 8,
//     zero-padded); it is decoded when the table is read, once per stretch of
//     equal IDs. With ODD the user is known not to be a plain (user, alias):
//     thread << 27 | index of a side record that holds the row.
const PRESENT: u64 = 1 << 63;
const VALID: u64 = 1 << 62;
const PURCH: u64 = 1 << 61;
const ODD: u64 = 1 << 60;
const LOCK: u64 = 1 << 59;
const ING_SHIFT: u32 = 31;
const ING_MASK: u64 = (1 << 28) - 1;
const IDX_BITS: u32 = 23;
const IDX_MASK: u64 = (1 << IDX_BITS) - 1;
const TS_MASK: u64 = 0x7FFF_FFFF;
/// The bytes of an ID of l bytes (2 to 9) after its first: a mask for the
/// eight bytes loaded there.
static TEXT_MASK: [u64; 10] = {
    let mut t = [0u64; 10];
    let mut l = 2;
    while l < 10 {
        t[l] = !0u64 >> (8 * (9 - l));
        l += 1;
    }
    t
};

/// Aliases of a user that the plain path knows (4 bits each in the user's
/// link word); a row or link with a higher alias is handled by text.
const ALIASES: u64 = 16;
/// The link word of a user: nibble a is the smallest alias linked with alias
/// a after `ul_flat` (a parent before). Stored xor UL_INIT, so zero pages
/// are "nothing linked".
const UL_INIT: u64 = 0xFEDC_BA98_7654_3210;
/// Table stores wait this many lines for their prefetched cell.
const RING: usize = 16;

type Cell = [AtomicU64; 2];

pub(crate) struct Keys {
    pub ekey: u64,
    pub ukey: u64,
    /// Cells of the event table (numbers from here on go by text).
    pub cap: usize,
    /// Users the plain path has room for.
    pub users: usize,
}

/// The keys of the generated IDs, from the first lines of the biggest events
/// file; None when the IDs do not have that shape.
pub(crate) fn detect(files: &[InFile], nthreads: usize, verbose: bool) -> Option<Keys> {
    // Off unless asked for (SS_DENSE=1): since scorer version 5 the rules say
    // that IDs must be treated as opaque strings, never as generator numbers.
    if std::env::var_os("SS_DENSE").is_none() {
        return None;
    }
    let biggest = files.iter().filter(|f| f.kind == Kind::Event && !f.map.is_null()).max_by_key(|f| f.len)?;
    let head = unsafe { std::slice::from_raw_parts(biggest.map, (biggest.len as usize).min(96 << 10)) };
    let last = head.iter().rposition(|&b| b == b'\n')?;
    let text = pad_buffer(head[..last + 1].to_vec());
    let begin = find_byte(&text, 0, b'\n') + 1;
    let (mut ev, mut us, mut lines) = (Vec::new(), Vec::new(), 0usize);
    scan_lines::<7, _>(&text, begin, text.len(), |f| {
        lines += 1;
        if ev.len() < 48 {
            if let Some(v) = id_number(b'e', f[0]) {
                ev.push(v);
            }
        }
        if us.len() < 64 {
            if let Some(v) = id_number(b'u', f[1]) {
                us.push(v);
            }
        }
    });
    if lines < 16 {
        return None;
    }
    // room for a quarter more events than the files seem to hold lines
    let avg = ((text.len() - begin) / lines).max(16);
    let bytes: u64 = files.iter().filter(|f| f.kind == Kind::Event).map(|f| f.len).sum();
    let cap = (bytes as usize / avg) * 5 / 4 + (1 << 16);
    let users = (cap / 16).clamp(1 << 12, 1 << IDX_BITS);
    let ukey = find_user_key(&us, users as u64, nthreads)?;
    let ekey = find_seq_key(&ev, cap as u64, nthreads)?;
    if verbose {
        eprintln!("  dense: event key {ekey:x} user key {ukey:x} cells {cap} users {users}");
    }
    Some(Keys { ekey, ukey, cap, users })
}

/// The key under which the sampled user IDs are (user, alias) with user below
/// `users` and alias below 16. Most rows use alias 0, whose number ends in
/// nine zero bits: the commonest low nine bits of the unmixed samples are the
/// key's, and with them only `users` candidates are left for one such sample.
fn find_user_key(sample: &[u64], users: u64, nthreads: usize) -> Option<u64> {
    if sample.len() < 8 {
        return None;
    }
    let ms: Vec<u64> = sample.iter().map(|&v| unmix(v)).collect();
    let mut cnt = [0u32; 512];
    for &m in &ms {
        cnt[(m & 511) as usize] += 1;
    }
    let klo = (0..512usize).max_by_key(|&c| cnt[c]).unwrap() as u64;
    let mut zero: Vec<u64> = ms.iter().copied().filter(|&m| m & 511 == klo).collect();
    zero.sort_unstable();
    zero.dedup();
    let bound = users << 9;
    let allowed = ms.len() / 8;
    let step = (SCR_C1 << 9) & M40;
    for pair in 0..(zero.len() / 2).min(3) {
        let (ma, delta) = (zero[2 * pair], zero[2 * pair] ^ zero[2 * pair + 1]);
        let found = AtomicU64::new(u64::MAX);
        let per = users.div_ceil(nthreads as u64);
        std::thread::scope(|sc| {
            for t in 0..nthreads as u64 {
                let (found, ms) = (&found, &ms);
                sc.spawn(move || {
                    let (lo, hi) = ((t * per).min(users), ((t + 1) * per).min(users));
                    // x = (user * 512) * C1 for the candidate user of the anchor
                    let mut x = lo.wrapping_mul(step) & M40;
                    for _ in lo..hi {
                        if (delta ^ x).wrapping_mul(SCR_C1I) & M40 < bound {
                            let key = ma ^ x;
                            let outside = ms
                                .iter()
                                .filter(|&&m| {
                                    let u = (m ^ key).wrapping_mul(SCR_C1I) & M40;
                                    u >= bound || (u & 511) >= ALIASES
                                })
                                .count();
                            if outside <= allowed {
                                found.fetch_min(key, Ordering::Relaxed);
                            }
                        }
                        x = x.wrapping_add(step) & M40;
                    }
                });
            }
        });
        let k = found.load(Ordering::Relaxed);
        if k != u64::MAX {
            return Some(k);
        }
    }
    None
}

struct Dx {
    ekey: u64,
    ukey: u64,
    cap: u64,
    users: u64,
    t: *const Cell,
    /// The link word of every user (see UL_INIT).
    ulink: *const AtomicU64,
    ws: i64,
    we: i64,
    /// Start of the click window.
    cws: i64,
    ing_base: i64,
    /// Campaign by number (see `camp_number`); 0xFFFF: none.
    camp_direct: Vec<u16>,
}

unsafe impl Sync for Dx {}

impl Dx {
    #[inline(always)]
    fn cell(&self, n: usize) -> &Cell {
        unsafe { &*self.t.add(n) }
    }
    #[inline(always)]
    fn link(&self, user: usize) -> &AtomicU64 {
        unsafe { &*self.ulink.add(user) }
    }
    #[inline(always)]
    fn fetch(&self, n: usize) {
        prefetch_w(self.t.wrapping_add(n));
    }
}

// --- user buckets: the unit of work of the session phase. The generator's
// first users are by far the busiest, so the first buckets are small.

const HEAD: [usize; 13] = [0, 1, 2, 3, 4, 5, 8, 16, 32, 64, 128, 256, 512];

static HEAD_LUT: [u8; 1024] = {
    let mut t = [0u8; 1024];
    let mut i = 0;
    while i < 1024 {
        let mut b = 0;
        while b + 1 < 13 && HEAD[b + 1] <= i {
            b += 1;
        }
        t[i] = b as u8;
        i += 1;
    }
    t
};

#[inline(always)]
fn bucket_of(user: usize) -> usize {
    if user < 1024 {
        HEAD_LUT[user] as usize
    } else {
        12 + (user >> 10)
    }
}

/// The bucket of a user and the user's place in it.
#[inline(always)]
fn bucket_local(user: usize) -> (usize, u64) {
    if user < 1024 {
        let b = HEAD_LUT[user] as usize;
        (b, (user - HEAD[b]) as u64)
    } else {
        (12 + (user >> 10), (user & 1023) as u64)
    }
}

/// Campaign IDs of the form `c_` + number are looked up by the number.
pub(crate) const CAMP_DIRECT: usize = 100_000;

/// The number of an ID that is `c_` + 1 to 5 digits without a leading zero
/// (two such IDs are equal exactly when their numbers are). `s` needs 8
/// readable bytes.
#[inline(always)]
pub(crate) fn camp_number(s: &[u8]) -> Option<usize> {
    let l = s.len();
    if l.wrapping_sub(3) > 4 {
        return None;
    }
    let x = load8(s, 0);
    let nd = l - 2;
    // the digits moved to the top bytes, '0' below them
    let d = ((x >> 16) << (8 * (8 - nd))) | (0x3030_3030_3030_3030u64 >> (8 * nd));
    if x as u16 != u16::from_le_bytes(*b"c_") || !all_digits(d, !0) || (nd > 1 && (x >> 16) as u8 == b'0') {
        return None;
    }
    Some(eight_digits(d) as usize)
}

fn bucket_start(b: usize) -> usize {
    if b < 13 {
        HEAD[b]
    } else {
        (b - 12) << 10
    }
}

// --- link words

#[inline(always)]
fn ul_find(x: u64, mut a: u64) -> u64 {
    loop {
        let p = (x >> (4 * a)) & 15;
        if p == a {
            return a;
        }
        a = p;
    }
}

/// Links aliases a and b: the larger root points at the smaller.
#[inline(always)]
fn ul_union(x: u64, a: u64, b: u64) -> u64 {
    let (ra, rb) = (ul_find(x, a), ul_find(x, b));
    if ra == rb {
        return x;
    }
    let (lo, hi) = (ra.min(rb), ra.max(rb));
    (x & !(15 << (4 * hi))) | (lo << (4 * hi))
}

/// Every nibble replaced by its root.
#[inline(always)]
fn ul_flat(x: u64) -> u64 {
    if x == UL_INIT {
        return x;
    }
    let mut y = 0;
    for a in 0..16 {
        y |= ul_find(x, a) << (4 * a);
    }
    y
}

/// The key of the ID `letter` + base 36 of v, as the generator prints it.
fn b36_key(letter: u8, v: u64) -> Key {
    let mut tmp = [0u8; 13];
    let mut at = 13;
    let mut r = v;
    loop {
        at -= 1;
        tmp[at] = b"0123456789abcdefghijklmnopqrstuvwxyz"[(r % 36) as usize];
        r /= 36;
        if r == 0 {
            break;
        }
    }
    let mut buf = [0u8; 32];
    buf[0] = letter;
    let n = 13 - at;
    buf[1..1 + n].copy_from_slice(&tmp[at..]);
    key_of(&buf[..n + 1])
}

#[inline(always)]
fn user_key(dx: &Dx, user: usize, alias: u64) -> Key {
    b36_key(b'u', scramble((user as u64) << 9 | alias, dx.ukey))
}

/// (user, alias) of the user ID `u` + the bytes of `text` if it is a plain
/// one: generated shape, user in range, alias below 16.
#[inline(always)]
fn text_user(dx: &Dx, text: u64) -> Option<(usize, u64)> {
    let n = 8 - (text.leading_zeros() / 8) as usize;
    let b = [text, 0u64];
    let s = unsafe { std::slice::from_raw_parts(b.as_ptr() as *const u8, n) };
    let u = seq_of(base36_40(s)?, dx.ukey);
    ((u >> 9) < dx.users && (u & 511) < ALIASES).then_some(((u >> 9) as usize, u & 15))
}

/// The key of the ID `letter` + the bytes of `text`.
fn text_key(letter: u8, text: u64) -> Key {
    let n = 8 - (text.leading_zeros() / 8) as usize;
    let mut buf = [0u8; 32];
    buf[0] = letter;
    buf[1..9].copy_from_slice(&text.to_le_bytes());
    key_of(&buf[..n + 1])
}

/// (user, alias) of a key that is a generated user ID with a user in range.
fn plain_user(dx: &Dx, k: &Key) -> Option<(usize, u64)> {
    if !is_short(k) {
        return None;
    }
    let mut b = Vec::with_capacity(48);
    key_bytes(k, &mut b);
    let n = b.len();
    b.resize(n + 32, 0);
    let u = seq_of(id_number(b'u', &b[..n])?, dx.ukey);
    ((u >> 9) < dx.users).then_some(((u >> 9) as usize, u & 511))
}

// --- phase 1 records

/// An event row that is not a plain cell: its user as written.
#[derive(Clone, Copy)]
struct SideEv {
    user: Key,
    ts: u32,
    purchase: u32,
    price: u32,
    cat: u32,
}

/// An event whose ID has no cell: deduplicated by the ID's text.
#[derive(Clone, Copy)]
struct OddEv {
    id: Key,
    ingest: u64,
    valid: bool,
    ev: SideEv,
}

/// A valid click that is not plain (its ID or its user).
#[derive(Clone, Copy)]
struct OddClick {
    user: Key,
    cid: Key,
    ts: u32,
    camp: u32,
}

/// An event / a click of a person handled by text.
#[derive(Clone, Copy)]
struct CEv {
    person: Key,
    ts: u32,
    purchase: u32,
    price: u32,
    cat: u32,
}

#[derive(Clone, Copy)]
struct CCl {
    person: Key,
    cid: Key,
    ts: u32,
    camp: u32,
}

struct D1 {
    odd_links: Vec<(Key, Key)>,
    /// Plain clicks by user bucket: [the bytes of the ID after its `k`, ts |
    /// campaign << 31 | alias << 47 | place of the user in the bucket << 51]
    /// (ts in ms from the start of the click window).
    clicks: Parts<[u64; 2], 4>,
    odd_clicks: Vec<OddClick>,
    /// Copies that found their cell taken.
    late: u64,
    side: Vec<SideEv>,
    odd_ev: Vec<OddEv>,
    /// One past the largest number stored.
    nmax: u64,
    lines: u64,
}

struct D1Out {
    odd_links: Vec<(Key, Key)>,
    clicks: Vec<PVec<[u64; 2]>>,
    odd_clicks: Vec<OddClick>,
    late: u64,
    side: Vec<SideEv>,
    odd_ev: Vec<OddEv>,
    nmax: u64,
    lines: u64,
}

struct Ring {
    n: [u64; RING],
    w0: [u64; RING],
    w1: [u64; RING],
    at: usize,
}

/// Stores a copy in its cell: the first copy of an event takes the empty
/// cell; a later one replaces what is there if its ingest time is smaller.
/// The writer holds the cell (LOCK in w0) while it writes the second word.
#[inline(always)]
unsafe fn commit(dx: &Dx, o: &mut D1, n: u64, w0: u64, w1: u64) {
    let cell = dx.cell(n as usize);
    if cell[0].compare_exchange(0, w0 | LOCK, Ordering::Acquire, Ordering::Relaxed).is_ok() {
        cell[1].store(w1, Ordering::Relaxed);
        cell[0].store(w0, Ordering::Release);
    } else {
        o.late += 1;
        commit_late(cell, w0, w1);
    }
}

#[cold]
#[inline(never)]
fn commit_late(cell: &Cell, w0: u64, w1: u64) {
    loop {
        let cur = cell[0].load(Ordering::Acquire);
        if cur & LOCK != 0 {
            std::hint::spin_loop();
            continue;
        }
        // (equal ingest times: the copies are identical, the first stays)
        if (w0 >> ING_SHIFT & ING_MASK) >= (cur >> ING_SHIFT & ING_MASK) {
            return;
        }
        if cell[0].compare_exchange(cur, w0 | LOCK, Ordering::Acquire, Ordering::Relaxed).is_ok() {
            cell[1].store(w1, Ordering::Relaxed);
            cell[0].store(w0, Ordering::Release);
            return;
        }
    }
}

impl Ring {
    #[inline(always)]
    unsafe fn push(&mut self, dx: &Dx, o: &mut D1, n: u64, w0: u64, w1: u64) {
        let i = self.at & (RING - 1);
        if self.at >= RING {
            commit(dx, o, *self.n.get_unchecked(i), *self.w0.get_unchecked(i), *self.w1.get_unchecked(i));
        }
        *self.n.get_unchecked_mut(i) = n;
        *self.w0.get_unchecked_mut(i) = w0;
        *self.w1.get_unchecked_mut(i) = w1;
        self.at += 1;
    }
    unsafe fn flush(&mut self, dx: &Dx, o: &mut D1) {
        for i in 0..self.at.min(RING) {
            commit(dx, o, self.n[i], self.w0[i], self.w1[i]);
        }
        self.at = 0;
    }
}

/// One event line (see the rules in README / rules.md: a copy takes part in
/// the dedup whatever it holds; `ok` says whether it would be a valid event).
#[inline(always)]
#[allow(clippy::too_many_arguments)]
unsafe fn event_line<'a, S: Fields<'a>>(dx: &Dx, ctx: &Ctx, dc: &mut DateCache, src: &mut S, o: &mut D1, ring: &mut Ring, w: usize, nmax: &mut u64) {
    let f0 = src.lo();
    if f0.is_empty() {
        return;
    }
    // the cell of a generated ID is fetched while the rest of the line is parsed
    let n = match id_number(b'e', f0) {
        Some(v) => seq_of(v, dx.ekey),
        None => u64::MAX,
    };
    if n < dx.cap {
        dx.fetch(n as usize);
    }
    let f1 = src.lo();
    let mut ok = !f1.is_empty();
    let f2 = src.lo();
    let ts = match parse_ts_dc(dc, f2) {
        Some(t) if t >= dx.ws && t < dx.we => t,
        _ => {
            ok = false;
            dx.ws
        }
    };
    let f3 = src.hi();
    let (ingest_s, ingest_ms) = parse_ingest_dc(dc, f3).unwrap_or((i64::MAX / 2000, i64::MAX / 2));
    let ingest = (ingest_s - dx.ing_base).clamp(0, ING_MASK as i64) as u64;
    ok &= ingest_ms - ts <= DAY;
    let f4 = src.hi();
    let ty = if f4.len() == 4 && (f4.as_ptr() as *const u32).read_unaligned() == u32::from_le_bytes(*b"view") { 0 } else { event_type(f4) };
    let f5 = src.hi();
    ok &= !f5.is_empty();
    let f6 = src.hi();
    let mut pc = 0u64;
    if ty == T_PURCHASE {
        match parse_price(f6) {
            Some(p) if ok => pc = (p as u64) << 8 | ctx.products.get(&key_of(f5)).unwrap_or(ctx.unknown_cat) as u64,
            _ => ok = false,
        }
    } else {
        ok &= ty < T_PURCHASE;
    }
    let purchase = (ty == T_PURCHASE) as u64;
    let ts_rel = (ts - dx.ws) as u64;
    let side = |f1: &[u8]| SideEv { user: key_of(f1), ts: ts_rel as u32, purchase: purchase as u32, price: (pc >> 8) as u32, cat: (pc & 0xFF) as u32 };
    if n < dx.cap {
        let head = PRESENT | ingest << ING_SHIFT;
        let (w0, w1) = if !ok {
            (head, 0)
        } else {
            let l1 = f1.len();
            let p1 = f1.as_ptr();
            if purchase == 0 && l1.wrapping_sub(2) < 8 && *p1 == b'u' && *p1.add(l1 - 1) != 0 {
                // the ID's bytes as they are (decoded when the table is read)
                (head | VALID | ts_rel, (p1.add(1) as *const u64).read_unaligned() & *TEXT_MASK.get_unchecked(l1))
            } else {
                let u = match id_number(b'u', f1) {
                    Some(v) => seq_of(v, dx.ukey),
                    None => u64::MAX,
                };
                if purchase != 0 && (u >> 9) < dx.users && (u & 511) < ALIASES {
                    (head | VALID | PURCH | ts_rel, pc << 32 | (u >> 9) << 4 | (u & 15))
                } else {
                    o.side.push(side(f1));
                    (head | VALID | ODD | ts_rel, (w as u64) << 27 | (o.side.len() - 1) as u64)
                }
            }
        };
        *nmax = (*nmax).max(n);
        ring.push(dx, o, n, w0, w1);
    } else {
        o.odd_ev.push(OddEv { id: key_of(f0), ingest, valid: ok, ev: side(f1) });
    }
}

/// The usual event line, a view or a cart of a user whose ID is `u` + up to 8
/// bytes with a generated event ID: the same cell as `event_line` writes, with
/// nothing kept for the rare cases. False: not such a line (nothing done).
#[inline(always)]
#[allow(clippy::too_many_arguments)]
unsafe fn event_fast(dx: &Dx, dc: &mut DateCache, p: *const u8, pr: *const u8, lo: *const Half, hi: *const Half, o: &mut D1, ring: &mut Ring, nmax: &mut u64) -> bool {
    let (lo, hi) = (&*lo, &*hi);
    let n = match id_number(b'e', std::slice::from_raw_parts(p.add(lo.s[0] as usize), lo.l[0] as usize)) {
        Some(v) => seq_of(v, dx.ekey),
        None => return false,
    };
    if n >= dx.cap {
        return false;
    }
    dx.fetch(n as usize);
    let ty = (pr.add(hi.s[1] as usize) as *const u32).read_unaligned();
    if hi.l[1] != 4 || (ty != u32::from_le_bytes(*b"view") && ty != u32::from_le_bytes(*b"cart")) {
        return false;
    }
    let l1 = lo.l[1] as usize;
    let p1 = p.add(lo.s[1] as usize);
    let mut ok = true;
    let mut text = 0u64;
    if l1.wrapping_sub(2) < 8 && *p1 == b'u' && *p1.add(l1 - 1) != 0 {
        text = (p1.add(1) as *const u64).read_unaligned() & *TEXT_MASK.get_unchecked(l1);
    } else if l1 == 0 {
        ok = false;
    } else {
        return false;
    }
    let ts = match parse_ts_dc(dc, std::slice::from_raw_parts(p.add(lo.s[2] as usize), lo.l[2] as usize)) {
        Some(t) if t >= dx.ws && t < dx.we => t,
        _ => {
            ok = false;
            dx.ws
        }
    };
    let (ingest_s, ingest_ms) =
        parse_ingest_dc(dc, std::slice::from_raw_parts(pr.add(hi.s[0] as usize), hi.l[0] as usize)).unwrap_or((i64::MAX / 2000, i64::MAX / 2));
    let ingest = (ingest_s - dx.ing_base).clamp(0, ING_MASK as i64) as u64;
    ok &= ingest_ms - ts <= DAY;
    ok &= hi.l[2] != 0;
    let head = PRESENT | ingest << ING_SHIFT;
    let (w0, w1) = if ok { (head | VALID | (ts - dx.ws) as u64, text) } else { (head, 0) };
    *nmax = (*nmax).max(n);
    ring.push(dx, o, n, w0, w1);
    true
}

/// Any other well-formed event line.
#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
unsafe fn event_rare(dx: &Dx, ctx: &Ctx, dc: &mut DateCache, p: *const u8, pr: *const u8, lo: *const Half, hi: *const Half, o: &mut D1, ring: &mut Ring, w: usize, nmax: &mut u64) {
    let mut f = MemoFields { p, pr, lo, hi, i: 0, j: 0, _buf: std::marker::PhantomData };
    event_line(dx, ctx, dc, &mut f, o, ring, w, nmax);
}

#[cold]
#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn event_slow(dx: &Dx, ctx: &Ctx, buf: &[u8], ls: usize, dc: &mut DateCache, o: &mut D1, ring: &mut Ring, w: usize, nmax: &mut u64) -> usize {
    let (end, res) = slow_line::<7>(buf, ls);
    if let Some(f) = res {
        unsafe { event_line(dx, ctx, dc, &mut ArrFields { f: &f, i: 0 }, o, ring, w, nmax) };
    }
    end + 1
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn parse_events(dx: &Dx, ctx: &Ctx, buf: &[u8], begin: usize, limit: usize, o: &mut D1, memo: &mut ShapeMemo, w: usize) {
    let n = buf.len();
    if begin >= n {
        return;
    }
    let mut ring = Ring { n: [0; RING], w0: [0; RING], w1: [0; RING], at: 0 };
    let mut dc = DateCache::new();
    let ptr = buf.as_ptr();
    let mut ls = begin;
    let mut lines = 0u64;
    // the largest number stored (plus one)
    let mut nmax = 0u64;
    while ls < limit && ls < n {
        let p = unsafe { ptr.add(ls) };
        prefetch(p.wrapping_add(INPUT_AHEAD));
        lines += 1;
        match unsafe { line_memo::<7, 3>(p, memo) } {
            LineM::Slow => ls = event_slow(dx, ctx, buf, ls, &mut dc, o, &mut ring, w, &mut nmax),
            LineM::Bad(e) => ls += e + 1,
            LineM::Good(e, r, lo, hi) => {
                ls += e + 1;
                unsafe {
                    let pr = p.add(r);
                    if !event_fast(dx, &mut dc, p, pr, lo, hi, o, &mut ring, &mut nmax) {
                        event_rare(dx, ctx, &mut dc, p, pr, lo, hi, o, &mut ring, w, &mut nmax);
                    }
                }
            }
        }
    }
    o.lines += lines;
    o.nmax = o.nmax.max(nmax + (lines != 0) as u64);
    unsafe { ring.flush(dx, o) };
}

/// One click line. Copies of a click are identical by the rules, and the
/// session code asks only for the latest click and the set of campaigns, so
/// clicks are not deduplicated at all.
#[inline(always)]
unsafe fn click_line<'a, S: Fields<'a>>(dx: &Dx, ctx: &Ctx, dc: &mut DateCache, src: &mut S, o: &mut D1) {
    let f0 = src.lo();
    let f1 = src.lo();
    if f0.is_empty() || f1.is_empty() {
        return;
    }
    let fc = src.hi();
    let camp = match camp_number(fc) {
        Some(n) => {
            let c = *dx.camp_direct.get_unchecked(n);
            if c == 0xFFFF {
                return;
            }
            c as u32
        }
        None => match ctx.campaigns.get(&key_of(fc)) {
            Some(c) if !fc.is_empty() => c,
            _ => return,
        },
    };
    let ts = match parse_ts_dc(dc, src.hi()) {
        Some(t) if t >= dx.cws && t < dx.we => (t - dx.cws) as u64,
        _ => return,
    };
    // a plain click: `k` + up to 8 bytes (kept as they are), a plain user
    let l0 = f0.len();
    let p0 = f0.as_ptr();
    if l0.wrapping_sub(2) < 8 && *p0 == b'k' && *p0.add(l0 - 1) != 0 {
        if let Some(uv) = id_number(b'u', f1) {
            let u = seq_of(uv, dx.ukey);
            if (u >> 9) < dx.users && (u & 511) < ALIASES {
                let text = (p0.add(1) as *const u64).read_unaligned() & *TEXT_MASK.get_unchecked(l0);
                let (b, local) = bucket_local((u >> 9) as usize);
                o.clicks.push(b, [text, ts | (camp as u64) << 31 | (u & 15) << 47 | local << 51]);
                return;
            }
        }
    }
    o.odd_clicks.push(OddClick { user: key_of(f1), cid: key_of(f0), ts: ts as u32, camp });
}

#[inline(never)]
fn parse_clicks(dx: &Dx, ctx: &Ctx, buf: &[u8], begin: usize, limit: usize, o: &mut D1, memo: &mut ShapeMemo) {
    let n = buf.len();
    if begin >= n {
        return;
    }
    let mut dc = DateCache::new();
    let ptr = buf.as_ptr();
    let mut ls = begin;
    while ls < limit && ls < n {
        let p = unsafe { ptr.add(ls) };
        prefetch(p.wrapping_add(INPUT_AHEAD));
        match unsafe { line_memo::<4, 2>(p, memo) } {
            LineM::Slow => {
                let (end, res) = slow_line::<4>(buf, ls);
                if let Some(f) = res {
                    unsafe { click_line(dx, ctx, &mut dc, &mut ArrFields { f: &f, i: 0 }, o) };
                }
                ls = end + 1;
            }
            LineM::Bad(e) => ls += e + 1,
            LineM::Good(e, r, lo, hi) => {
                ls += e + 1;
                let mut f = MemoFields { p, pr: unsafe { p.add(r) }, lo, hi, i: 0, j: 0, _buf: std::marker::PhantomData };
                unsafe { click_line(dx, ctx, &mut dc, &mut f, o) };
            }
        }
    }
}

#[inline(never)]
fn parse_links(dx: &Dx, buf: &[u8], begin: usize, limit: usize, o: &mut D1) {
    scan_lean::<2, 1, _>(buf, begin, limit, #[inline(always)] |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        if let (Some(a), Some(b)) = (id_number(b'u', f[0]), id_number(b'u', f[1])) {
            let (ua, ub) = (seq_of(a, dx.ukey), seq_of(b, dx.ukey));
            // the same user and both aliases below 16
            if (ua ^ ub) < ALIASES && (ua & 511) < ALIASES && (ua >> 9) < dx.users {
                if ua != ub {
                    // (the word is shared by the threads: unions commute)
                    let word = dx.link((ua >> 9) as usize);
                    let mut old = word.load(Ordering::Relaxed);
                    loop {
                        let new = ul_union(old ^ UL_INIT, ua & 15, ub & 15) ^ UL_INIT;
                        if new == old {
                            break;
                        }
                        match word.compare_exchange_weak(old, new, Ordering::Relaxed, Ordering::Relaxed) {
                            Ok(_) => break,
                            Err(x) => old = x,
                        }
                    }
                }
                return;
            }
        }
        let (ka, kb) = (key_of(f[0]), key_of(f[1]));
        if ka != kb {
            o.odd_links.push((ka, kb));
        }
    });
}

/// `cl`: clicks as ts << 32 | index, to be sorted by ts with equal times by
/// click ID descending (the last of equal times has the smallest ID).
/// `id_desc(i, j)` orders two indices by ID, descending.
fn sort_clicks(cl: &mut [u64], id_desc: impl Fn(usize, usize) -> std::cmp::Ordering) {
    if cl.len() < 2 {
        return;
    }
    if cl.len() <= 20 {
        for i in 1..cl.len() {
            let v = cl[i];
            let mut j = i;
            while j > 0 && cl[j - 1] > v {
                cl[j] = cl[j - 1];
                j -= 1;
            }
            cl[j] = v;
        }
    } else {
        cl.sort_unstable();
    }
    let mut a = 0;
    while a < cl.len() {
        let mut b = a + 1;
        while b < cl.len() && cl[b] >> 32 == cl[a] >> 32 {
            b += 1;
        }
        if b - a > 1 {
            cl[a..b].sort_unstable_by(|x, y| id_desc(*x as u32 as usize, *y as u32 as usize));
        }
        a = b;
    }
}

/// The sessions of one person whose events are `words` (ts | PURCHASE) in
/// table order, with the purchases' (price, category) in `lpur`.
#[inline(always)]
fn one_person(geo: &Geo, st: &mut P3, name: Key, words: &[u32], lpur: &[(u32, u32)], cls: &[u64], camps: &[u32]) {
    let (mut in_order, mut prev, mut k) = (true, 0u32, 0u32);
    for &wd in words {
        let t = wd & TS_DEAD;
        in_order &= t >= prev;
        prev = t;
        if wd & PURCHASE != 0 {
            st.purs.push(((t as u64) << 32) | (PURCHASE | k) as u64);
            k += 1;
        }
    }
    if in_order {
        person_finish(geo, st, name, lpur, cls, camps, Some(words));
    } else {
        st.purs.clear();
        person_add_run(st, words, 0);
        person_finish(geo, st, name, lpur, cls, camps, None);
    }
}

/// `letter` + base 36 of v, as the generator prints it, into buf; the length.
#[inline(always)]
fn id_text(letter: u8, v: u64, buf: &mut [u8; 16]) -> usize {
    // the digits end at tmp[16]; the bytes after them stay zero
    let mut tmp = [0u8; 24];
    let mut at = 16;
    let mut r = v & M40;
    loop {
        at -= 1;
        let q = r / 36;
        let d = (r - q * 36) as u8;
        tmp[at] = if d < 10 { b'0' + d } else { b'a' - 10 + d };
        r = q;
        if r == 0 {
            break;
        }
    }
    // (a number below 2^40 has at most 8 digits)
    buf[0] = letter;
    let w = u64::from_le_bytes(tmp[at..at + 8].try_into().unwrap());
    buf[1..9].copy_from_slice(&w.to_le_bytes());
    17 - at
}

/// The ID text (the bytes after `u`, zero-padded) of alias `alias` of `user`.
#[inline(always)]
fn user_text(dx: &Dx, user: usize, alias: u64) -> u64 {
    let mut buf = [0u8; 16];
    id_text(b'u', scramble((user as u64) << 9 | alias, dx.ukey), &mut buf);
    u64::from_le_bytes(buf[1..9].try_into().unwrap())
}

/// The name of the person made of the aliases of `user` linked with alias
/// `comp` (the smallest of them): the smallest of their IDs, into buf; its
/// length. `texts[a]` is the text of alias a where `known` has bit a.
#[inline(always)]
fn person_name(dx: &Dx, user: usize, comp: u64, flat: u64, texts: &[u64; 16], known: u32, buf: &mut [u8; 16]) -> usize {
    let get = |a: u64| if known >> a & 1 != 0 { texts[a as usize] } else { user_text(dx, user, a) };
    let mut best = get(comp);
    if flat != UL_INIT {
        for a in comp + 1..16 {
            if (flat >> (4 * a)) & 15 == comp {
                let t = get(a);
                // (zero padding: byte order of the IDs is the order of these)
                if t.swap_bytes() < best.swap_bytes() {
                    best = t;
                }
            }
        }
    }
    buf[0] = b'u';
    buf[1..9].copy_from_slice(&best.to_le_bytes());
    9 - (best.leading_zeros() / 8) as usize
}

/// The sessions of the person whose events are `words` (ts | PURCHASE), in
/// time order (checked by the caller), with the purchases' (price, category)
/// in `lpur`. `cls`: the person's clicks as ts << 32 | index into `ucl`, in
/// time order, equal times by click ID descending. The same bookkeeping as
/// `person_finish`.
/// From cell n on: skips the cells that are no events and takes the views and
/// carts whose user bytes are `text` (their ts goes to `evs`), up to a valid
/// cell that is anything else, or `hi`. Returns where it stopped.
#[inline(never)]
#[allow(clippy::too_many_arguments)]
unsafe fn same_text_run(t: *const Cell, mut n: usize, hi: usize, text: u64, evs: *mut u32, len: &mut usize, prev: &mut u64, in_order: &mut bool) -> usize {
    let (mut l, mut p, mut ok) = (*len, *prev, *in_order);
    while n < hi {
        let c = &*t.add(n);
        let w0 = c[0].load(Ordering::Relaxed);
        if w0 & VALID != 0 {
            if w0 & (ODD | PURCH) != 0 || c[1].load(Ordering::Relaxed) != text {
                break;
            }
            let ts = w0 & TS_MASK;
            ok &= ts >= p;
            p = ts;
            *evs.add(l) = ts as u32;
            l += 1;
        }
        n += 1;
    }
    *len = l;
    *prev = p;
    *in_order = ok;
    n
}

/// Per campaign, the number of the last session it was counted for.
struct Stamps {
    of: Vec<u32>,
    serial: u32,
}

#[inline(never)]
#[allow(clippy::too_many_arguments)]
fn person_words(g: &Geo, st: &mut P3, stamp: &mut Stamps, words: &[u32], lpur: &[(u32, u32)], cls: &[u64], ucl: &[[u64; 2]], name: &[u8]) {
    let camp_of = |e: u64| unsafe { (ucl.get_unchecked(e as u32 as usize)[1] >> 31 & 0xFFFF) as usize };
    let mut lo = 0usize;
    let mut hi = 0usize;
    let mut cur_day: i64 = i64::MIN;
    let mut d_sessions = 0u32;
    let mut d_events = 0u32;
    let mut d_rev = 0u64;
    let mut s_start: i64;
    let mut s_didx: usize;
    let mut s_rev: u64;
    let mut s_purch: bool;
    let stride = g.organic as usize + 1;
    // A person with few clicks (nearly everyone): the clicks' times and
    // campaigns side by side, and every session looks at its window directly
    // (a campaign counts once: it is stamped with the session's number).
    // Many clicks: the window slides over them with a count per campaign.
    const FEW: usize = 64;
    let few = cls.len() <= FEW;
    let mut cts = [0i64; FEW];
    let mut ccamp = [0u32; FEW];
    if few {
        for (i, &e) in cls.iter().enumerate() {
            cts[i] = (e >> 32) as i64;
            ccamp[i] = camp_of(e) as u32;
        }
    }
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
            if few {
                unsafe {
                    while hi < cls.len() && *cts.get_unchecked(hi) <= sc {
                        hi += 1;
                    }
                    while lo < hi && *cts.get_unchecked(lo) < sc - WEEK {
                        lo += 1;
                    }
                    // every campaign of the window once
                    let row = s_didx * stride;
                    stamp.serial += 1;
                    let serial = stamp.serial;
                    for i in lo..hi {
                        let c = *ccamp.get_unchecked(i) as usize;
                        let seen = stamp.of.get_unchecked_mut(c);
                        if *seen != serial {
                            *seen = serial;
                            *st.assist.get_unchecked_mut(row + c) += 1;
                        }
                    }
                }
            } else {
                while hi < cls.len() && ((cls[hi] >> 32) as i64) <= sc {
                    let c = camp_of(cls[hi]);
                    if st.cnt[c] == 0 {
                        flush_assists!();
                        st.pos[c] = st.active.len() as u32;
                        st.active.push(c as u32);
                    }
                    st.cnt[c] += 1;
                    hi += 1;
                }
                while lo < hi && ((cls[lo] >> 32) as i64) < sc - WEEK {
                    let c = camp_of(cls[lo]);
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
            }
            let credit = if lo < hi { camp_of(cls[hi - 1]) } else { g.organic as usize };
            let cell = s_didx * stride + credit;
            unsafe { *st.sess.get_unchecked_mut(cell) += 1 };
            if s_purch {
                let a = unsafe { st.agg.get_unchecked_mut(cell) };
                a[1] += 1;
                a[2] += s_rev;
            }
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
            if d_rev != 0 {
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
            }
            st.uw.row((cur_day - g.day0) as usize, d_sessions, d_events, d_rev, top);
        }};
    }
    // a session at a time: its first event, then the events less than 30
    // minutes after the one before
    let n = words.len();
    let mut pi = 0usize;
    let mut i = 0usize;
    while i < n {
        let t = unsafe { (*words.get_unchecked(i) & TS_DEAD) as i64 };
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
        let mut prev = t;
        let mut j = i;
        loop {
            let wd = unsafe { *words.get_unchecked(j) };
            if wd & PURCHASE != 0 {
                let (p, cat) = unsafe { *lpur.get_unchecked(pi) };
                pi += 1;
                let (p, cat) = (p as u64, cat as usize);
                s_rev += p;
                s_purch = true;
                d_rev += p;
                if st.catsum[cat] == 0 {
                    st.touched.push(cat as u32);
                }
                st.catsum[cat] += p;
            }
            j += 1;
            if j == n {
                break;
            }
            let tn = unsafe { (*words.get_unchecked(j) & TS_DEAD) as i64 };
            if tn - prev > GAP {
                break;
            }
            prev = tn;
        }
        d_events += (j - i) as u32;
        i = j;
        finish_session!();
    }
    if n > 0 {
        flush_day!();
        st.uw.end_person(name);
    }
    if !few {
        flush_assists!();
        for &c in &st.active {
            st.cnt[c as usize] = 0;
        }
        st.active.clear();
    }
}

#[inline(always)]
fn hsel(k: &Key, n: usize) -> usize {
    ((hk(k) >> 32) as usize) % n
}

/// Runs the whole pipeline; false if the data is not laid out the way the
/// engine needs (nothing is left in the output folders then).
#[allow(clippy::too_many_arguments)]
pub(crate) fn run(keys: &Keys, ctx: &Ctx, files: &[InFile], chunks: &[Chunk], out: &Path, nthreads: usize, verbose: bool, t0: Instant) -> bool {
    let (ws, we) = (ctx.ws, ctx.we);
    let ncamp = ctx.camp_ids.len();
    // what the packed records have room for
    if we - ws + WEEK >= 1 << 31 || ncamp + 1 >= 1 << 16 || ctx.cat_names.len() > 255 || nthreads > 31 {
        return false;
    }
    let lap = |what: &str| {
        if verbose {
            let mut ru = [0i64; 18];
            unsafe { getrusage(0, &mut ru) };
            eprintln!(
                "{:>8.1} ms  dense: {what}  (user {:.2}s sys {:.2}s minflt {})",
                t0.elapsed().as_secs_f64() * 1e3,
                ru[0] as f64 + ru[1] as f64 * 1e-6,
                ru[2] as f64 + ru[3] as f64 * 1e-6,
                ru[8]
            );
        }
    };
    lap("start");
    let cap = keys.cap;
    let users = keys.users;
    let table: &'static mut [[u64; 2]] = zeroed_slice(cap);
    let ulink: &'static mut [u64] = zeroed_slice(users);
    let dx = Dx {
        ekey: keys.ekey,
        ukey: keys.ukey,
        cap: cap as u64,
        users: users as u64,
        t: table.as_ptr() as *const Cell,
        ulink: ulink.as_ptr() as *const AtomicU64,
        ws,
        we,
        cws: ws - WEEK,
        ing_base: ws.div_euclid(1000) - (1 << 27),
        camp_direct: {
            let mut t = vec![0xFFFFu16; CAMP_DIRECT];
            for (i, id) in ctx.camp_ids.iter().enumerate() {
                let mut b = id.clone();
                b.resize(id.len() + 16, 0);
                if let Some(n) = camp_number(&b[..id.len()]) {
                    if t[n] == 0xFFFF {
                        t[n] = i as u16;
                    }
                }
            }
            t
        },
    };
    let nb = bucket_of(users - 1) + 1;
    let bytes_of = |k: Kind| files.iter().filter(|f| f.kind == k).map(|f| f.len).sum::<u64>() as usize;
    let est_clicks = bytes_of(Kind::Click) / 40;
    // click buffers: the head buckets hold far more than their share
    let click_res = est_clicks * 3 / nb / nthreads + 64;

    // --- phase 1: every line once
    let next = AtomicUsize::new(0);
    let p1: Vec<D1Out> = run_workers(nthreads, |w| {
        let mut buf = Vec::new();
        let mut o = D1 {
            odd_links: Vec::new(),
            clicks: Parts::new_by(nb, |q| if q < 40 { click_res * 16 } else { click_res }),
            odd_clicks: Vec::new(),
            late: 0,
            side: Vec::new(),
            odd_ev: Vec::new(),
            nmax: 0,
            lines: 0,
        };
        let mut memo_ev = ShapeMemo::new();
        let mut memo_cl = ShapeMemo::new();
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            if i >= chunks.len() {
                break;
            }
            let c = &chunks[i];
            let inf = &files[c.file];
            let (bytes, b, l) = chunk_bytes(inf, c, &mut buf);
            match c.kind {
                Kind::Link => parse_links(&dx, bytes, b, l, &mut o),
                Kind::Click => parse_clicks(&dx, ctx, bytes, b, l, &mut o, &mut memo_cl),
                Kind::Event => parse_events(&dx, ctx, bytes, b, l, &mut o, &mut memo_ev, w),
            }
            if !inf.map.is_null() {
                release(unsafe { inf.map.add(c.start as usize) }, (c.end - c.start) as usize);
            }
        }
        D1Out {
            odd_links: o.odd_links,
            clicks: o.clicks.finish(),
            odd_clicks: o.odd_clicks,
            late: o.late,
            side: o.side,
            odd_ev: o.odd_ev,
            nmax: o.nmax,
            lines: o.lines,
        }
    });
    lap("phase 1 parse");
    let nmax = p1.iter().map(|o| o.nmax).max().unwrap_or(0) as usize;
    if p1.iter().any(|o| o.side.len() >= 1 << 27) {
        return false;
    }

    // --- who is handled by text ("complex"): users with a link that is not
    // between two of their first 16 aliases, and users of rows that are not
    // plain.
    let mut complex = vec![0u64; users / 64 + 1];
    let mut mark = |k: &Key| {
        if let Some((user, _)) = plain_user(&dx, k) {
            complex[user >> 6] |= 1 << (user & 63);
        }
    };
    for o in &p1 {
        for (a, b) in &o.odd_links {
            mark(a);
            mark(b);
        }
        for c in &o.odd_clicks {
            mark(&c.user);
        }
    }
    // events without a cell: the copy with the smallest ingest time, by ID
    let mut odd_best: HashMap<Key, OddEv> = HashMap::new();
    for o in &p1 {
        for e in &o.odd_ev {
            match odd_best.get_mut(&e.id) {
                Some(b) => {
                    if e.ingest < b.ingest {
                        *b = *e;
                    }
                }
                None => {
                    odd_best.insert(e.id, *e);
                }
            }
        }
    }
    let odd_winners: Vec<SideEv> = odd_best.values().filter(|e| e.valid).map(|e| e.ev).collect();
    for e in &odd_winners {
        mark(&e.user);
    }
    let is_complex = |user: usize| complex[user >> 6] >> (user & 63) & 1 != 0;
    // the general union-find: the links by text, and the links inside the
    // complex users
    let mut pairs: Vec<(Key, Key)> = Vec::new();
    for o in &p1 {
        pairs.extend_from_slice(&o.odd_links);
    }
    let ulink: &[u64] = ulink;
    for (wi, &word) in complex.iter().enumerate() {
        let mut word = word;
        while word != 0 {
            let user = wi * 64 + word.trailing_zeros() as usize;
            word &= word - 1;
            // every alias with the one it points at
            let x = ulink[user] ^ UL_INIT;
            for a in 1..16u64 {
                let p = (x >> (4 * a)) & 15;
                if p != a {
                    pairs.push((user_key(&dx, user, a), user_key(&dx, user, p)));
                }
            }
        }
    }
    let mut gids: KeyMap<u32> = KeyMap::new(pairs.len() * 2 + 16);
    let mut parent: Vec<u32> = Vec::new();
    let mut gmin: Vec<Key> = Vec::new();
    fn gfind(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = p;
        }
        x
    }
    for (a, b) in &pairs {
        let mut node = |k: &Key| {
            let (id, fresh) = gids.get_or_insert(k, parent.len() as u32);
            if fresh {
                parent.push(id);
                gmin.push(*k);
            }
            id
        };
        let (ia, ib) = (node(a), node(b));
        let (ra, rb) = (gfind(&mut parent, ia), gfind(&mut parent, ib));
        if ra != rb {
            parent[rb as usize] = ra;
            if cmp_key(&gmin[rb as usize], &gmin[ra as usize]) == std::cmp::Ordering::Less {
                gmin[ra as usize] = gmin[rb as usize];
            }
        }
    }
    // the person (name) of every linked ID
    let gname: Vec<Key> = (0..parent.len() as u32).map(|i| gmin[gfind(&mut parent, i) as usize]).collect();
    let person_of = |k: &Key| match gids.get(k) {
        Some(i) => gname[i as usize],
        None => *k,
    };
    let mut seed_cev: Vec<Vec<CEv>> = vec![Vec::new(); nthreads];
    let mut seed_ccl: Vec<Vec<CCl>> = vec![Vec::new(); nthreads];
    for e in &odd_winners {
        let person = person_of(&e.user);
        seed_cev[hsel(&person, nthreads)].push(CEv { person, ts: e.ts, purchase: e.purchase, price: e.price, cat: e.cat });
    }
    for o in &p1 {
        for c in &o.odd_clicks {
            let person = person_of(&c.user);
            seed_ccl[hsel(&person, nthreads)].push(CCl { person, cid: c.cid, ts: c.ts, camp: c.camp });
        }
    }
    if verbose {
        let lines: u64 = p1.iter().map(|o| o.lines).sum();
        let dups: u64 = p1.iter().map(|o| o.late).sum();
        let nc = complex.iter().map(|w| w.count_ones() as usize).sum::<usize>();
        eprintln!(
            "  dense: {lines} event lines, top number {nmax}, {dups} late copies, {} side rows, {} rows without a cell, {} odd clicks, {} links by text, {nc} complex users",
            p1.iter().map(|o| o.side.len()).sum::<usize>(),
            p1.iter().map(|o| o.odd_ev.len()).sum::<usize>(),
            p1.iter().map(|o| o.odd_clicks.len()).sum::<usize>(),
            pairs.len(),
        );
    }
    lap("identities by text");

    // --- phase 2: sessions, a bucket of users at a time
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let geo = Geo { ws, day0, organic: ncamp as u32 };
    let nbuckets = ((((we - ws).clamp(0, 1 << 31) / GAP) as usize + 2) + 63) / 64 * 64;
    let _ = fs::create_dir_all(out.join("user_daily"));
    let _ = fs::create_dir_all(out.join("campaign_daily"));
    let cell = |n: usize| dx.cell(n);
    // The user of the first plain event at or after cell p.
    let key_at = |p: usize| -> u64 {
        let mut q = p;
        while q < nmax {
            let c = cell(q);
            let w0 = c[0].load(Ordering::Relaxed);
            if w0 & (VALID | ODD) == VALID {
                let w1 = c[1].load(Ordering::Relaxed);
                if w0 & PURCH != 0 {
                    return (w1 >> 4) & IDX_MASK;
                }
                if let Some((user, _)) = text_user(&dx, w1) {
                    return user as u64;
                }
            }
            q += 1;
        }
        u64::MAX
    };
    // Where bucket b starts in the table, if the table is in user order
    // (the scan of every bucket checks that it is).
    let bounds: Vec<AtomicU64> = (0..=nb).map(|_| AtomicU64::new(u64::MAX)).collect();
    let bound_of = |b: usize| -> usize {
        if b == 0 {
            return 0;
        }
        if b >= nb {
            return nmax;
        }
        let known = bounds[b].load(Ordering::Relaxed);
        if known != u64::MAX {
            return known as usize;
        }
        let target = bucket_start(b) as u64;
        let (mut lo, mut hi) = (0usize, nmax);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if key_at(mid) < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        bounds[b].store(lo as u64, Ordering::Relaxed);
        lo
    };
    let bad = AtomicBool::new(false);
    let next = AtomicUsize::new(0);
    let barrier = std::sync::Barrier::new(nthreads);
    let end_barrier = std::sync::Barrier::new(nthreads);
    let shared_cev: Vec<Mutex<Vec<Vec<CEv>>>> = (0..nthreads).map(|_| Mutex::new(Vec::new())).collect();
    let shared_ccl: Vec<Mutex<Vec<Vec<CCl>>>> = (0..nthreads).map(|_| Mutex::new(Vec::new())).collect();
    let aggs: Vec<Vec<[u64; 4]>> = run_workers(nthreads, |w| {
        let mut st = p3_new(out, w, ctx, day0, ndays, nbuckets);
        let mut cev: Vec<Vec<CEv>> = vec![Vec::new(); nthreads];
        let mut ccl: Vec<Vec<CCl>> = vec![Vec::new(); nthreads];
        // the clicks of a bucket grouped by user (`cstart`), and one person's
        // clicks as ts << 32 | index, in time order
        let mut ucl: Vec<[u64; 2]> = Vec::new();
        let (mut cstart, mut fill): (Vec<u32>, Vec<u32>) = (Vec::new(), Vec::new());
        let mut cls: Vec<u64> = Vec::new();
        // (the rare ways) a person's events and purchases, the campaigns and
        // places of the person's clicks
        let mut words: Vec<u32> = Vec::new();
        let mut lpur: Vec<(u32, u32)> = Vec::new();
        let (mut camps, mut cidx): (Vec<u32>, Vec<u32>) = (Vec::new(), Vec::new());
        // the events of the stretch being read (ts | PURCHASE) and its purchases
        let mut evs: Vec<u32> = Vec::new();
        let mut epur: Vec<(u32, u32)> = Vec::new();
        let mut stamp = Stamps { of: vec![0u32; ncamp + 1], serial: 0 };
        let mut seen;
        let mut namebuf = [0u8; 16];
        'units: loop {
            let b = next.fetch_add(1, Ordering::Relaxed);
            if b >= nb || bad.load(Ordering::Relaxed) {
                break;
            }
            let (ua, ub) = (bucket_start(b), bucket_start(b + 1).min(users));
            let nu = ub - ua;
            // --- the bucket's clicks, grouped by user
            cstart.clear();
            cstart.resize(nu + 1, 0);
            let nc: usize = p1.iter().map(|o| o.clicks[b].len()).sum();
            if nc > 0 {
                for o in p1.iter() {
                    for r in o.clicks[b].as_slice() {
                        cstart[(r[1] >> 51) as usize + 1] += 1;
                    }
                }
                for k in 0..nu {
                    cstart[k + 1] += cstart[k];
                }
                fill.clear();
                fill.extend_from_slice(&cstart[..nu]);
                ucl.clear();
                ucl.reserve(nc);
                unsafe { ucl.set_len(nc) };
                for o in p1.iter() {
                    for r in o.clicks[b].as_slice() {
                        let f = &mut fill[(r[1] >> 51) as usize];
                        ucl[*f as usize] = *r;
                        *f += 1;
                    }
                }
            }
            // --- the bucket's events, user by user
            let (lo, hi) = (bound_of(b), bound_of(b + 1));
            seen = [0u64; 16];
            let mut n = lo;
            evs.clear();
            evs.reserve(hi - lo);
            // What the valid cell (w0, w1) is: Some((user, alias)) for a plain
            // event; None for a row by text, which is handed to its person
            // and taken out of the table.
            macro_rules! plain_or_out {
                ($c:expr, $w0:expr, $w1:expr) => {{
                    let (c, w0, w1): (&Cell, u64, u64) = ($c, $w0, $w1);
                    let ua = if w0 & ODD != 0 {
                        None
                    } else if w0 & PURCH != 0 {
                        Some((((w1 >> 4) & IDX_MASK) as usize, w1 & 15))
                    } else {
                        text_user(&dx, w1)
                    };
                    if ua.is_none() {
                        let (user, purchase, price, cat) = if w0 & ODD != 0 {
                            let s = &p1[(w1 >> 27 & 31) as usize].side[(w1 & 0x7FF_FFFF) as usize];
                            (s.user, s.purchase, s.price, s.cat)
                        } else {
                            (text_key(b'u', w1), 0, 0, 0)
                        };
                        let person = person_of(&user);
                        cev[hsel(&person, nthreads)].push(CEv { person, ts: (w0 & TS_MASK) as u32, purchase, price, cat });
                        c[0].store(PRESENT, Ordering::Relaxed);
                    }
                    ua
                }};
            }
            // the plain event at n, when it is known already
            let mut next_ev: Option<(usize, u64)> = None;
            'events: loop {
                // --- the first event of a stretch
                let (user, alias) = loop {
                    if let Some(x) = next_ev.take() {
                        break x;
                    }
                    if n >= hi {
                        break 'events;
                    }
                    let c = cell(n);
                    let w0 = c[0].load(Ordering::Relaxed);
                    if w0 & VALID != 0 {
                        if let Some(x) = plain_or_out!(c, w0, c[1].load(Ordering::Relaxed)) {
                            break x;
                        }
                    }
                    n += 1;
                };
                // every user in one stretch of the table, inside its bucket
                let lu = user.wrapping_sub(ua);
                if lu >= nu || seen[lu >> 6] >> (lu & 63) & 1 != 0 {
                    bad.store(true, Ordering::Relaxed);
                    break 'units;
                }
                seen[lu >> 6] |= 1 << (lu & 63);
                let flat = ul_flat(ulink[user] ^ UL_INIT);
                let start = n;
                // the ID texts of the aliases met in the stretch; the last one
                let mut texts = [0u64; 16];
                let mut known = 0u32;
                let mut last_text = 0u64;
                let (mut in_order, mut prev, mut aset) = (true, 0u64, 1u32 << alias);
                epur.clear();
                {
                    let c = cell(n);
                    if c[0].load(Ordering::Relaxed) & PURCH == 0 {
                        last_text = c[1].load(Ordering::Relaxed);
                        texts[alias as usize] = last_text;
                        known |= 1 << alias;
                    }
                }
                // --- to the end of the stretch: is it in time order?
                let mut len = 0usize;
                loop {
                    // the events with the ID of the one before, in one go
                    n = unsafe { same_text_run(dx.t, n, hi, last_text, evs.as_mut_ptr(), &mut len, &mut prev, &mut in_order) };
                    if n >= hi {
                        break;
                    }
                    // a valid cell with another ID, a purchase or a row by text
                    let c = cell(n);
                    let w0 = c[0].load(Ordering::Relaxed);
                    let w1 = c[1].load(Ordering::Relaxed);
                    let mut found = None;
                    if w0 & (ODD | PURCH) == 0 {
                        let mut k = known;
                        while k != 0 {
                            let a = k.trailing_zeros() as usize;
                            k &= k - 1;
                            if texts[a] == w1 {
                                found = Some((user, a as u64));
                                break;
                            }
                        }
                    }
                    if found.is_none() {
                        found = plain_or_out!(c, w0, w1);
                    }
                    match found {
                        None => {
                            n += 1;
                            continue;
                        }
                        Some((u2, a2)) => {
                            if u2 != user {
                                next_ev = Some((u2, a2));
                                break;
                            }
                            if w0 & PURCH == 0 {
                                texts[a2 as usize] = w1;
                                known |= 1 << a2;
                                last_text = w1;
                            } else {
                                epur.push(((w1 >> 40) as u32, (w1 >> 32 & 0xFF) as u32));
                            }
                            aset |= 1 << a2;
                        }
                    }
                    let t = w0 & TS_MASK;
                    in_order &= t >= prev;
                    prev = t;
                    // (room for every cell of the bucket was reserved)
                    unsafe { *evs.as_mut_ptr().add(len) = (t | (w0 >> 30 & 0x8000_0000)) as u32 };
                    len += 1;
                    n += 1;
                }
                unsafe { evs.set_len(len) };
                let end = n;
                // the alias of a plain event of the stretch
                let alias_of = |w0: u64, w1: u64| {
                    if w0 & PURCH != 0 {
                        w1 & 15
                    } else {
                        (0..16u64).find(|&a| known >> a & 1 != 0 && texts[a as usize] == w1).unwrap_or(0)
                    }
                };
                let plain = |m: usize| {
                    let c = cell(m);
                    let w0 = c[0].load(Ordering::Relaxed);
                    (w0 & (VALID | ODD) == VALID).then(|| (w0, c[1].load(Ordering::Relaxed)))
                };
                if is_complex(user) {
                    let mut ckey: [Option<Key>; 16] = [None; 16];
                    for m in start..end {
                        if let Some((w0, w1)) = plain(m) {
                            let alias = alias_of(w0, w1);
                            let person = match ckey[alias as usize] {
                                Some(k) => k,
                                None => {
                                    let k = person_of(&user_key(&dx, user, alias));
                                    ckey[alias as usize] = Some(k);
                                    k
                                }
                            };
                            let (purchase, price, cat) = if w0 & PURCH != 0 { (1, (w1 >> 40) as u32, (w1 >> 32 & 0xFF) as u32) } else { (0, 0, 0) };
                            cev[hsel(&person, nthreads)].push(CEv { person, ts: (w0 & TS_MASK) as u32, purchase, price, cat });
                        }
                    }
                    continue;
                }
                // one person if the aliases met are all linked
                let comp_of = |alias: u64| (flat >> (4 * alias)) & 15;
                let comp0 = comp_of(aset.trailing_zeros() as u64);
                let mut multi = false;
                if aset & (aset - 1) != 0 {
                    let mut k = aset;
                    while k != 0 {
                        multi |= comp_of(k.trailing_zeros() as u64) != comp0;
                        k &= k - 1;
                    }
                }
                let mine = cstart[lu] as usize..cstart[lu + 1] as usize;
                if !multi && in_order {
                    cls.clear();
                    for i in mine {
                        let r = unsafe { ucl.get_unchecked(i) };
                        if comp_of(r[1] >> 47 & 15) == comp0 {
                            cls.push((r[1] & TS_MASK) << 32 | i as u64);
                        }
                    }
                    sort_clicks(&mut cls, |i, j| ucl[j][0].swap_bytes().cmp(&ucl[i][0].swap_bytes()));
                    let len = person_name(&dx, user, comp0, flat, &texts, known, &mut namebuf);
                    person_words(&geo, &mut st, &mut stamp, &evs, &epur, &cls, &ucl, &namebuf[..len]);
                    continue;
                }
                // Some alias with events is not linked to the others, or the
                // events are not in time order: a person at a time, sorted.
                let mut todo = 0u32;
                let mut k = aset;
                while k != 0 {
                    todo |= 1 << comp_of(k.trailing_zeros() as u64);
                    k &= k - 1;
                }
                while todo != 0 {
                    let comp = todo.trailing_zeros() as u64;
                    todo &= todo - 1;
                    words.clear();
                    lpur.clear();
                    for m in start..end {
                        if let Some((w0, w1)) = plain(m) {
                            if comp_of(alias_of(w0, w1)) == comp {
                                let purchase = w0 & PURCH != 0;
                                words.push((w0 & TS_MASK) as u32 | (purchase as u32) << 31);
                                if purchase {
                                    lpur.push(((w1 >> 40) as u32, (w1 >> 32 & 0xFF) as u32));
                                }
                            }
                        }
                    }
                    cls.clear();
                    camps.clear();
                    cidx.clear();
                    for i in mine.clone() {
                        let r = &ucl[i];
                        if comp_of(r[1] >> 47 & 15) == comp {
                            cls.push((r[1] & TS_MASK) << 32 | camps.len() as u64);
                            camps.push((r[1] >> 31 & 0xFFFF) as u32);
                            cidx.push(i as u32);
                        }
                    }
                    sort_clicks(&mut cls, |i, j| ucl[cidx[j] as usize][0].swap_bytes().cmp(&ucl[cidx[i] as usize][0].swap_bytes()));
                    let len = person_name(&dx, user, comp, flat, &texts, known, &mut namebuf);
                    one_person(&geo, &mut st, key_of(&namebuf[..len]), &words, &lpur, &cls, &camps);
                }
            }
            // the clicks of the bucket's complex users
            if nc > 0 {
                for lu in 0..nu {
                    if cstart[lu + 1] > cstart[lu] && is_complex(ua + lu) {
                        for r in &ucl[cstart[lu] as usize..cstart[lu + 1] as usize] {
                            let person = person_of(&user_key(&dx, ua + lu, r[1] >> 47 & 15));
                            ccl[hsel(&person, nthreads)].push(CCl { person, cid: text_key(b'k', r[0]), ts: (r[1] & TS_MASK) as u32, camp: (r[1] >> 31 & 0xFFFF) as u32 });
                        }
                    }
                }
            }
        }
        *shared_cev[w].lock().unwrap() = cev;
        *shared_ccl[w].lock().unwrap() = ccl;
        barrier.wait();
        // --- the persons handled by text: those whose name hashes to this worker
        if !bad.load(Ordering::Relaxed) {
            let mut ids: HashMap<Key, u32> = HashMap::new();
            let mut names: Vec<Key> = Vec::new();
            let mut pev: Vec<Vec<u64>> = Vec::new();
            let mut pcl: Vec<Vec<(u32, u32, Key)>> = Vec::new();
            let mut lpur: Vec<(u32, u32)> = Vec::new();
            let mut take_ev = |list: &[CEv]| {
                for e in list {
                    let p = *ids.entry(e.person).or_insert_with(|| {
                        names.push(e.person);
                        pev.push(Vec::new());
                        pcl.push(Vec::new());
                        (names.len() - 1) as u32
                    });
                    let x = if e.purchase != 0 {
                        lpur.push((e.price, e.cat));
                        PURCHASE | (lpur.len() - 1) as u32
                    } else {
                        0
                    };
                    pev[p as usize].push((e.ts as u64) << 32 | x as u64);
                }
            };
            take_ev(&seed_cev[w]);
            for s in &shared_cev {
                let list = std::mem::take(&mut s.lock().unwrap()[w]);
                take_ev(&list);
            }
            let mut take_cl = |list: &[CCl]| {
                for c in list {
                    if let Some(&p) = ids.get(&c.person) {
                        pcl[p as usize].push((c.ts, c.camp, c.cid));
                    }
                }
            };
            take_cl(&seed_ccl[w]);
            for s in &shared_ccl {
                let list = std::mem::take(&mut s.lock().unwrap()[w]);
                take_cl(&list);
            }
            let mut pcamps: Vec<u32> = Vec::new();
            for p in 0..names.len() {
                let cl = &pcl[p];
                cls.clear();
                pcamps.clear();
                for (i, c) in cl.iter().enumerate() {
                    cls.push((c.0 as u64) << 32 | i as u64);
                    pcamps.push(c.1);
                }
                sort_clicks(&mut cls, |i, j| cmp_key(&cl[j].2, &cl[i].2));
                person_add(&mut st, &pev[p]);
                person_finish(&geo, &mut st, names[p], &lpur, &cls, &pcamps, None);
            }
        }
        st.uw.finish(w == 0);
        if verbose {
            eprintln!("  dense w{w}: {} user_daily rows in {} files, done at {:.1} ms", st.uw.rows, st.uw.files, t0.elapsed().as_secs_f64() * 1e3);
        }
        end_barrier.wait();
        release_sub_arena(w);
        p3_agg(st)
    });
    if bad.load(Ordering::Relaxed) {
        // not in user order: the general pipeline starts over
        if verbose {
            eprintln!("  dense: the table is not in user order, giving up");
        }
        let _ = fs::remove_dir_all(out.join("user_daily"));
        release(table.as_ptr() as *const u8, cap * 16);
        return false;
    }
    lap("phase 2 sessions + user_daily");
    write_campaign_daily(out, ctx, &aggs, day0, ndays);
    true
}
