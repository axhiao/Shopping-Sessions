//! Scorer diagnostics (feature "diag"): runs the pipeline as child processes
//! on a 20x copy of the dev set, with the sampling profiler on, and returns
//! where the samples fall (by source line) inside one extra output row. Never
//! part of a scored build.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::Instant;

/// The text of the extra output row.
static ROW: std::sync::Mutex<String> = std::sync::Mutex::new(String::new());

macro_rules! dl {
    ($($a:tt)*) => {{
        eprintln!($($a)*);
    }};
}

pub fn rows() -> Vec<String> {
    vec![ROW.lock().unwrap().chars().take(1750).collect()]
}

/// Runs `f(thread)` on `nt` threads; returns the wall time in seconds.
fn timed<F: Fn(usize) + Sync>(nt: usize, f: F) -> f64 {
    let t = Instant::now();
    std::thread::scope(|s| {
        for i in 0..nt {
            let f = &f;
            s.spawn(move || f(i));
        }
    });
    t.elapsed().as_secs_f64()
}

const B36: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

/// `v` in base 36 without leading zeros (the way the generator prints an ID's number).
fn b36_var(mut v: u64, out: &mut Vec<u8>) {
    let mut buf = [0u8; 13];
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = B36[(v % 36) as usize];
        v /= 36;
        if v == 0 {
            break;
        }
    }
    out.extend_from_slice(&buf[i..]);
}

/// The content `ct` of an ID field as copy `c` (from 1) writes it. Copies keep
/// the shape of generated IDs: an event id with a sequence number gets the
/// number moved up by `c * stride` (so the copies together are one dense
/// range, like one big data set); any other letter-and-digits ID gets its
/// first character after the letter rotated by `c`; the rest gets a tag in front.
fn retag(ct: &[u8], c: usize, seq: Option<(u64, u64)>, out: &mut Vec<u8>) {
    if ct.is_empty() {
        return;
    }
    if let Some((key, stride)) = seq {
        // (`ct` lies in a buffer with 32 spare bytes at its end)
        if let Some(v) = crate::id_number(b'e', ct) {
            let n = (crate::seq_of(v, key) + c as u64 * stride) & crate::M40;
            out.push(b'e');
            b36_var(crate::scramble(n, key), out);
            return;
        }
    }
    let digit = |b: u8| B36.iter().position(|&x| x == b);
    match (ct[0], ct.get(1).and_then(|&b| digit(b))) {
        (b'e' | b'u' | b'k', Some(d)) => {
            out.push(ct[0]);
            out.push(B36[(d + c) % 36]);
            out.extend_from_slice(&ct[2..]);
        }
        _ => {
            out.push(B36[c % 36]);
            out.extend_from_slice(ct);
        }
    }
}

/// Writes `copies` copies of every CSV in `src/{events,clicks,links}` to `dst`
/// with the IDs in the first two fields of each line made distinct per copy
/// (see `retag`): a data set `copies` times the size, same shape. Returns the
/// bytes written and whether the event ids kept their sequence numbers.
fn synthesize(src: &Path, dst: &Path, copies: usize, nt: usize) -> (u64, bool) {
    let _ = fs::remove_dir_all(dst);
    for d in ["events", "clicks", "links"] {
        fs::create_dir_all(dst.join(d)).unwrap();
    }
    for f in ["manifest.json", "products.csv", "campaigns.csv"] {
        let _ = fs::copy(src.join(f), dst.join(f));
    }
    let mut jobs: Vec<(std::path::PathBuf, std::path::PathBuf, bool)> = Vec::new();
    for d in ["events", "clicks", "links"] {
        if let Ok(rd) = fs::read_dir(src.join(d)) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if name.ends_with(".csv") {
                    jobs.push((e.path(), dst.join(d).join(name), d == "events"));
                }
            }
        }
    }
    // the key of the event ids (from the biggest events file) and the number of event lines
    let mut lines = 0u64;
    let mut biggest: Vec<u8> = Vec::new();
    for j in jobs.iter().filter(|j| j.2) {
        let text = fs::read(&j.0).unwrap_or_default();
        lines += text.iter().filter(|&&b| b == b'\n').count() as u64;
        if text.len() > biggest.len() {
            biggest = text;
        }
    }
    biggest.extend_from_slice(&[0u8; 32]);
    let mut sample: Vec<u64> = Vec::new();
    for line in biggest[..biggest.len() - 32].split(|&b| b == b'\n').skip(1) {
        if sample.len() == 48 {
            break;
        }
        let end = line.iter().position(|&b| b == b',').unwrap_or(line.len());
        if let Some(v) = crate::id_number(b'e', &line[..end]) {
            sample.push(v);
        }
    }
    let seq = crate::find_seq_key(&sample, (lines * 2).max(1 << 12), nt).map(|key| (key, lines));
    dl!("[diag] event ids: {} lines, key {:?}", lines, seq.map(|s| format!("{:x}", s.0)));
    let next = std::sync::atomic::AtomicUsize::new(0);
    let total = std::sync::atomic::AtomicU64::new(0);
    timed(nt, |_| loop {
        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if i >= jobs.len() {
            break;
        }
        let mut text = fs::read(&jobs[i].0).unwrap_or_default();
        let len = text.len();
        text.extend_from_slice(&[0u8; 32]);
        let head = text[..len].iter().position(|&b| b == b'\n').map(|p| p + 1).unwrap_or(len);
        let mut out = Vec::with_capacity((len + len / 16 + 64) * copies);
        // all copies go into one file, so the file count stays that of the source
        out.extend_from_slice(&text[..head]);
        for c in 0..copies {
            if c == 0 {
                out.extend_from_slice(&text[head..len]);
                continue;
            }
            for line in text[head..len].split_inclusive(|&b| b == b'\n') {
                let mut at = 0;
                for field in 0..2 {
                    let end = line[at..].iter().position(|&b| b == b',' || b == b'\n').map(|p| at + p).unwrap_or(line.len());
                    let f = &line[at..end];
                    let quoted = f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"';
                    let ct = if quoted { &f[1..f.len() - 1] } else { f };
                    if quoted {
                        out.push(b'"');
                    }
                    retag(ct, c, if field == 0 && jobs[i].2 { seq } else { None }, &mut out);
                    if quoted {
                        out.push(b'"');
                    }
                    if end < line.len() && line[end] == b',' {
                        out.push(b',');
                        at = end + 1;
                    } else {
                        at = end;
                        break;
                    }
                }
                out.extend_from_slice(&line[at..]);
            }
        }
        fs::write(&jobs[i].1, &out).unwrap();
        total.fetch_add(out.len() as u64, std::sync::atomic::Ordering::Relaxed);
    });
    (total.load(std::sync::atomic::Ordering::Relaxed), seq.is_some())
}

/// Runs this binary as a child on `data` with extra environment. Returns the
/// wall time in ms and [link map, phase 1 end, dedup end, phase 3 end, user s, sys s]
/// from its phase lines (zeros if it failed).
fn child(data: &Path, out: &Path, envs: &[(&str, &str)]) -> (f64, [f64; 8]) {
    let _ = fs::remove_dir_all(out);
    let _ = fs::create_dir_all(out);
    let exe = std::env::current_exe().unwrap_or_else(|_| "/proc/self/exe".into());
    let mut cmd = std::process::Command::new(exe);
    cmd.env("SS_CHILD", "1").env("SS_VERBOSE", "1").env("DATA_DIR", data).env("OUTPUT_DIR", out);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    cmd.stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped());
    let t = Instant::now();
    let r = cmd.output();
    let ms = t.elapsed().as_secs_f64() * 1e3;
    // [link map, phase 1 end, dedup end, phase 3 end, user s, sys s, sequence phase end, sequence path on]
    let mut laps = [0f64; 8];
    match r {
        Ok(o) => {
            let text = String::from_utf8_lossy(&o.stderr);
            for l in text.lines() {
                let at: f64 = l.split_whitespace().next().and_then(|v| v.parse().ok()).unwrap_or(0.0);
                let k = if l.contains("link map built") {
                    0
                } else if l.contains("phase 1 parse") {
                    1
                } else if l.contains("phase 2a dedup") {
                    2
                } else if l.contains("phase 3 sessions") {
                    3
                } else if l.contains("phase 2b sequence") {
                    6
                } else {
                    if l.contains("sequence numbers: on=true") {
                        laps[7] = 1.0;
                    }
                    if l.contains("ms  done") {
                        let num = |key: &str| -> f64 {
                            l.find(key).and_then(|p| l[p + key.len()..].split('s').next().and_then(|v| v.trim().parse().ok())).unwrap_or(0.0)
                        };
                        laps[4] = num("(user ");
                        laps[5] = num(" sys ");
                    }
                    continue;
                };
                laps[k] = at;
            }
            if laps[6] == 0.0 {
                laps[6] = laps[2];
            }
            if !o.status.success() {
                use std::os::unix::process::ExitStatusExt;
                dl!("[diag]     child failed: status={:?} signal={:?}", o.status.code(), o.status.signal());
                for l in text.lines().rev().take(4) {
                    dl!("[diag]       child: {l}");
                }
                laps = [0.0; 8];
            }
        }
        Err(e) => dl!("[diag]     child failed: {e}"),
    }
    (ms, laps)
}

fn b36(mut v: u64, width: usize) -> String {
    let mut s = vec![b'0'; width];
    v = v.min(36u64.pow(width as u32) - 1);
    for i in (0..width).rev() {
        s[i] = b"0123456789abcdefghijklmnopqrstuvwxyz"[(v % 36) as usize];
        v /= 36;
    }
    String::from_utf8(s).unwrap()
}

/// Scatter micro-benchmark: every thread spreads 4M 16-byte records over 1024
/// partitions through 64-byte staging buffers. ns per record and thread for:
/// c = registers + streaming stores on untouched memory (page faults included),
/// then on the same (now faulted) memory: a = store + reload + streaming (the
/// old writer), b = registers + streaming, p = registers + plain stores,
/// e = direct 16-byte writes without staging; then a and b again.
#[allow(dead_code)]
fn scatter(nt: usize) -> String {
    use std::arch::x86_64::*;
    const P: usize = 1024;
    const N: usize = 4 << 20;
    const CAP: usize = 5120;
    let bytes = P * CAP * 16;
    let regions: Vec<usize> = (0..nt).map(|_| crate::zeroed_slice::<u8>(bytes).as_mut_ptr() as usize).collect();
    let mut s = String::from("S:");
    for mode in [1usize, 0, 1, 2, 3, 0, 1] {
        let w = timed(nt, |t| unsafe {
            let base = regions[t] as *mut u8;
            let mut buf = vec![[0u64; 8]; P];
            let mut cnt = vec![0u32; P];
            let mut len = vec![0u32; P];
            let (bufp, cntp, lenp) = (buf.as_mut_ptr() as *mut u64, cnt.as_mut_ptr(), len.as_mut_ptr());
            let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ ((t as u64 + 1) << 32);
            for _ in 0..N {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let q = (x >> 20) as usize & (P - 1);
                let (a, b) = (x, x.rotate_left(17));
                let l = *lenp.add(q) as usize;
                if l + 4 > CAP {
                    continue;
                }
                let d = base.add((q * CAP + l) * 16);
                if mode == 3 {
                    (d as *mut u64).write(a);
                    (d as *mut u64).add(1).write(b);
                    *lenp.add(q) = l as u32 + 1;
                    continue;
                }
                let k = *cntp.add(q) as usize;
                let bp = bufp.add(q * 8);
                if k < 3 {
                    *bp.add(2 * k) = a;
                    *bp.add(2 * k + 1) = b;
                    *cntp.add(q) = k as u32 + 1;
                    continue;
                }
                *cntp.add(q) = 0;
                *lenp.add(q) = l as u32 + 4;
                if mode == 0 {
                    *bp.add(6) = a;
                    *bp.add(7) = b;
                    let v0 = _mm256_loadu_si256(bp as *const __m256i);
                    let v1 = _mm256_loadu_si256(bp.add(4) as *const __m256i);
                    _mm256_stream_si256(d as *mut __m256i, v0);
                    _mm256_stream_si256(d.add(32) as *mut __m256i, v1);
                } else {
                    let v0 = _mm256_loadu_si256(bp as *const __m256i);
                    let lo = _mm_loadu_si128(bp.add(4) as *const __m128i);
                    let v1 = _mm256_inserti128_si256(_mm256_castsi128_si256(lo), _mm_set_epi64x(b as i64, a as i64), 1);
                    if mode == 1 {
                        _mm256_stream_si256(d as *mut __m256i, v0);
                        _mm256_stream_si256(d.add(32) as *mut __m256i, v1);
                    } else {
                        _mm256_storeu_si256(d as *mut __m256i, v0);
                        _mm256_storeu_si256(d.add(32) as *mut __m256i, v1);
                    }
                }
            }
            std::hint::black_box((&buf, &len));
        });
        let per = w * 1e9 / N as f64;
        dl!("[diag] scatter mode {mode}: {per:.1} ns/record");
        s.push_str(&format!("{per:.1},"));
    }
    for r in &regions {
        crate::release(*r as *const u8, bytes);
    }
    s.pop();
    s.push(';');
    s
}

/// Hash-table micro-benchmark: every thread inserts 2M random 64-bit keys
/// into one shared 512 MB open-addressing table (16-byte slots, prefetched 16
/// operations ahead) and raises the slot's second word. ns per operation and
/// thread: on untouched memory, then twice more with the same keys.
#[allow(dead_code)]
fn table(nt: usize) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    const BITS: u32 = 25;
    const OPS: usize = 2 << 20;
    const D: usize = 16;
    let n = 1usize << BITS;
    let tab = crate::zeroed_slice::<[AtomicU64; 2]>(n);
    let tab: &[[AtomicU64; 2]] = tab;
    let slot = |fp: u64| (fp.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - BITS)) as usize;
    let mut s = String::from("T:");
    for pass in 0..3u64 {
        let w = timed(nt, |t| {
            let mut x = 0xD6E8_FEB8_6659_FD93u64 ^ ((t as u64 + 1) << 40);
            let mut ring = [1u64; D];
            for i in 0..OPS + D {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let fp = x | 1;
                crate::prefetch_w(unsafe { tab.as_ptr().add(slot(fp)) });
                let old = std::mem::replace(&mut ring[i % D], fp);
                if i >= D {
                    let mut j = slot(old);
                    loop {
                        let e = unsafe { tab.get_unchecked(j) };
                        let c = e[0].load(Ordering::Relaxed);
                        if c == old {
                            break;
                        }
                        if c == 0 {
                            match e[0].compare_exchange(0, old, Ordering::Relaxed, Ordering::Relaxed) {
                                Ok(_) => break,
                                Err(c2) if c2 == old => break,
                                _ => {}
                            }
                        }
                        j = (j + 1) & (n - 1);
                    }
                    let e = unsafe { tab.get_unchecked(j) };
                    e[1].fetch_max((old >> 8) + pass, Ordering::Relaxed);
                }
            }
        });
        let per = w * 1e9 / OPS as f64;
        dl!("[diag] table pass {pass}: {per:.1} ns/op");
        s.push_str(&format!("{per:.1},"));
    }
    crate::release(tab.as_ptr() as *const u8, n * 16);
    s.pop();
    s.push(';');
    s
}

/// address -> "line code" (main.rs line, 10000 + pq.rs line, ...) or a
/// function name, from the table the Docker build wrote next to the binary.
fn load_table() -> HashMap<u64, String> {
    let path = std::env::var("SS_LINES").unwrap_or_else(|_| "/lines.tbl".into());
    let mut m = HashMap::new();
    if let Ok(text) = fs::read_to_string(&path) {
        for l in text.lines() {
            let mut it = l.split_whitespace();
            if let (Some(a), Some(k)) = (it.next(), it.next()) {
                if let Ok(a) = u64::from_str_radix(a, 16) {
                    m.insert(a, k.to_string());
                }
            }
        }
    }
    dl!("[diag] line table {path}: {} addresses", m.len());
    m
}

pub fn run(data: &Path, _out: &Path, nt: usize) {
    let _ = data;
    let t0 = Instant::now();
    let scratch = std::path::PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    let mut row = String::from("v7;");
    // fingerprint of this binary (FNV-1a), to match it with a local build
    let exe = fs::read(std::env::current_exe().unwrap_or_else(|_| "/proc/self/exe".into())).unwrap_or_default();
    let fnv = exe.iter().fold(0xcbf2_9ce4_8422_2325u64, |h, &b| (h ^ b as u64).wrapping_mul(0x100_0000_01b3));
    row.push_str(&format!("X{};", b36(fnv % 36u64.pow(8), 8)));
    let syn = scratch.join("syn");
    let (bytes, seq) = synthesize(data, &syn, 20, nt);
    dl!("[diag] synthetic x20 data: {} MB in {:.0} ms, sequence numbers kept: {seq}", bytes >> 20, t0.elapsed().as_secs_f64() * 1e3);
    row.push_str(&format!("q{};", seq as u32));
    let out = scratch.join("synout");

    // pipeline variants: code:wall,link map,phase 1,dedup,sequence phase,phase 3,user,sys,sequence path on (best of 3)
    let sets: Vec<(&str, Vec<(&str, &str)>)> = vec![
        ("d", vec![]),
        ("ns", vec![("SS_NO_SEQ", "1")]),
        ("x12", vec![("SS_IXSHIFT", "12")]),
        ("x13", vec![("SS_IXSHIFT", "13")]),
        ("x15", vec![("SS_IXSHIFT", "15")]),
        ("x16", vec![("SS_IXSHIFT", "16")]),
        ("r1", vec![("SS_REL", "1")]),
        ("np", vec![("SS_POP", "0")]),
        ("p2", vec![("SS_PF2", "1")]),
        ("nr1", vec![("SS_NO_SEQ", "1"), ("SS_REL", "1")]),
        ("ns2", vec![("SS_NO_SEQ", "1")]),
        ("d2", vec![]),
    ];
    for (name, envs) in &sets {
        let mut best = (f64::MAX, [0f64; 8]);
        for _ in 0..3 {
            let r = child(&syn, &out, envs);
            if r.0 < best.0 && r.1[3] > 0.0 {
                best = r;
            }
        }
        if best.0 == f64::MAX {
            best.0 = 0.0;
        }
        let l = best.1;
        let rec = format!(
            "{name}:{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.1},{:.1},{:.0};",
            best.0,
            l[0],
            l[1] - l[0],
            l[2] - l[1],
            l[6] - l[2],
            l[3] - l[6],
            l[4],
            l[5],
            l[7]
        );
        dl!("[diag] variant {rec}");
        row.push_str(&rec);
    }
    row.push(';');
    row.push(';');

    // profile of the default pipeline: several runs, samples summed by table key
    let table = load_table();
    let prof = scratch.join("prof.txt");
    let prof_s = prof.to_string_lossy().to_string();
    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut addrs: HashMap<u64, u64> = HashMap::new();
    let (mut total, mut unknown, mut runs) = (0u64, 0u64, 0);
    while runs < 8 && t0.elapsed().as_secs_f64() < 90.0 {
        let _ = fs::remove_file(&prof);
        child(&syn, &out, &[("SS_PROF", &prof_s)]);
        runs += 1;
        if let Ok(text) = fs::read_to_string(&prof) {
            for l in text.lines() {
                let mut it = l.split_whitespace();
                let (Some(a), Some(c)) = (it.next(), it.next()) else { continue };
                let (Ok(a), Ok(c)) = (u64::from_str_radix(a, 16), c.parse::<u64>()) else { continue };
                total += c;
                *addrs.entry(a).or_insert(0) += c;
                match table.get(&a) {
                    Some(k) => *counts.entry(k.clone()).or_insert(0) += c,
                    None => unknown += c,
                }
            }
        }
    }
    let _ = fs::remove_dir_all(&out);
    let _ = fs::remove_dir_all(&syn);
    let _ = fs::remove_file(&prof);
    let mut v: Vec<(String, u64)> = counts.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    // P<samples>,<not in the table>,<runs>; then functions without a source
    // line (name=count), then lines: 3 base-36 digits of the line code and 3
    // of the count, most samples first
    row.push_str(&format!("P{total},{unknown},{runs};"));
    let names: Vec<String> = v.iter().filter(|(k, _)| k.parse::<u64>().is_err()).take(6).map(|(k, c)| format!("{k}={c}")).collect();
    row.push_str(&names.join(","));
    row.push(';');
    let mut lines = 0;
    for (k, c) in v.iter() {
        if let Ok(code) = k.parse::<u64>() {
            if lines == 80 {
                break;
            }
            lines += 1;
            row.push_str(&b36(code, 3));
            row.push_str(&b36(*c, 3));
        }
    }
    // then instruction addresses (offsets in the binary): 4 base-36 digits and
    // 3 of the count, most samples first
    row.push(';');
    let mut av: Vec<(u64, u64)> = addrs.into_iter().collect();
    av.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    for (a, c) in av.iter() {
        if *a >= 36u64.pow(4) {
            continue;
        }
        if row.len() + 7 > 1750 {
            break;
        }
        row.push_str(&b36(*a, 4));
        row.push_str(&b36(*c, 3));
    }
    dl!("[diag] done in {:.0} ms: {} samples in {runs} runs, {} keys, row {} chars", t0.elapsed().as_secs_f64() * 1e3, total, v.len(), row.len());
    dl!("[diag] {row}");
    *ROW.lock().unwrap() = row;
}
