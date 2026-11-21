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

/// Writes `copies` tagged copies of every CSV in `src/{events,clicks,links}` to
/// `dst` (the first two fields of each line get a one-character tag, so IDs
/// stay unique per copy): a data set `copies` times the size, same shape.
fn synthesize(src: &Path, dst: &Path, copies: usize, nt: usize) -> u64 {
    let _ = fs::remove_dir_all(dst);
    for d in ["events", "clicks", "links"] {
        fs::create_dir_all(dst.join(d)).unwrap();
    }
    for f in ["manifest.json", "products.csv", "campaigns.csv"] {
        let _ = fs::copy(src.join(f), dst.join(f));
    }
    let mut jobs: Vec<(std::path::PathBuf, std::path::PathBuf)> = Vec::new();
    for d in ["events", "clicks", "links"] {
        if let Ok(rd) = fs::read_dir(src.join(d)) {
            for e in rd.flatten() {
                let name = e.file_name().to_string_lossy().to_string();
                if name.ends_with(".csv") {
                    jobs.push((e.path(), dst.join(d).join(name)));
                }
            }
        }
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let total = std::sync::atomic::AtomicU64::new(0);
    timed(nt, |_| loop {
        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if i >= jobs.len() {
            break;
        }
        let text = fs::read(&jobs[i].0).unwrap_or_default();
        let head = text.iter().position(|&b| b == b'\n').map(|p| p + 1).unwrap_or(text.len());
        let mut out = Vec::with_capacity((text.len() + text.len() / 16 + 64) * copies);
        // all copies go into one file, so the file count stays that of the source
        out.extend_from_slice(&text[..head]);
        for c in 0..copies {
            if c == 0 {
                out.extend_from_slice(&text[head..]);
            } else {
                let tag = b"0123456789abcdefghijklmnopqrstuvwxyz"[c % 36];
                for line in text[head..].split_inclusive(|&b| b == b'\n') {
                    // tag the first two fields (after an opening quote, and only if not empty)
                    let mut at = 0;
                    for _ in 0..2 {
                        let end = line[at..].iter().position(|&b| b == b',' || b == b'\n').map(|p| at + p).unwrap_or(line.len());
                        let f = &line[at..end];
                        if f.is_empty() || f == b"\"\"" {
                            out.extend_from_slice(f);
                        } else if f[0] == b'"' {
                            out.push(b'"');
                            out.push(tag);
                            out.extend_from_slice(&f[1..]);
                        } else {
                            out.push(tag);
                            out.extend_from_slice(f);
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
        }
        fs::write(&jobs[i].1, &out).unwrap();
        total.fetch_add(out.len() as u64, std::sync::atomic::Ordering::Relaxed);
    });
    total.load(std::sync::atomic::Ordering::Relaxed)
}

/// Runs this binary as a child on `data` with extra environment. Returns the
/// wall time in ms and [link map, phase 1 end, dedup end, phase 3 end, user s, sys s]
/// from its phase lines (zeros if it failed).
fn child(data: &Path, out: &Path, envs: &[(&str, &str)]) -> (f64, [f64; 6]) {
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
    let mut laps = [0f64; 6];
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
                } else {
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
            if !o.status.success() {
                use std::os::unix::process::ExitStatusExt;
                dl!("[diag]     child failed: status={:?} signal={:?}", o.status.code(), o.status.signal());
                for l in text.lines().rev().take(4) {
                    dl!("[diag]       child: {l}");
                }
                laps = [0.0; 6];
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

/// Scanner and field-parsing micro-benchmarks on the first megabyte of the
/// biggest events file: ns per line and thread, on 1 and on `nt` threads.
/// o = plain scanner, l = lean scanner (every field used); O / L = the same
/// plus the old / new field parsing.
fn micro(data: &Path, nt: usize) -> String {
    let mut files: Vec<(std::path::PathBuf, u64)> = Vec::new();
    if let Ok(rd) = fs::read_dir(data.join("events")) {
        for e in rd.flatten() {
            files.push((e.path(), e.metadata().map(|m| m.len()).unwrap_or(0)));
        }
    }
    let Some((p, _)) = files.iter().max_by_key(|f| f.1) else { return String::new() };
    let mut text = fs::read(p).unwrap_or_default();
    text.truncate(text.len().min(1 << 20));
    while !text.is_empty() && text.last() != Some(&b'\n') {
        text.pop();
    }
    if text.len() < 1000 {
        return String::new();
    }
    let buf = crate::pad_buffer(text);
    let begin = crate::find_byte(&buf, 0, b'\n') + 1;
    let mut s = String::new();
    for threads in [1, nt] {
        for kind in 0..4 {
            let lines = std::sync::atomic::AtomicU64::new(0);
            let w = timed(threads, |_| {
                let (mut n, mut acc) = (0u64, 0u64);
                let mut dc = crate::DateCache::new();
                for _ in 0..40 {
                    match kind {
                        0 => crate::scan_lines::<7, _>(&buf, begin, buf.len(), |fs| {
                            n += 1;
                            for x in fs {
                                acc = acc.wrapping_add(x.len() as u64 ^ x.as_ptr() as u64);
                            }
                        }),
                        1 => crate::scan_lean::<7, 3, _>(&buf, begin, buf.len(), |fs| {
                            n += 1;
                            for x in fs {
                                acc = acc.wrapping_add(x.len() as u64 ^ x.as_ptr() as u64);
                            }
                        }),
                        2 => crate::scan_lines::<7, _>(&buf, begin, buf.len(), |fs| {
                            n += 1;
                            let k = crate::key_of(fs[0]);
                            let u = crate::key_of(fs[1]);
                            let ts = crate::parse_ts_fast(fs[2]).unwrap_or(1);
                            let ing = crate::parse_ingest_s(fs[3]).unwrap_or((2, 2));
                            let ty = crate::event_type(fs[4]);
                            let pr = if ty == 2 { crate::parse_price(fs[6]).unwrap_or(7) } else { 0 };
                            acc = acc.wrapping_add(k[0] ^ k[1] ^ u[0] ^ u[1] ^ ts as u64 ^ ing.0 as u64 ^ ty as u64 ^ pr as u64);
                        }),
                        _ => crate::scan_lean::<7, 3, _>(&buf, begin, buf.len(), |fs| {
                            n += 1;
                            let k = crate::key_of(fs[0]);
                            let u = crate::key_of(fs[1]);
                            let ts = crate::parse_ts_dc(&mut dc, fs[2]).unwrap_or(1);
                            let ing = crate::parse_ingest_dc(&mut dc, fs[3]).unwrap_or((2, 2));
                            let ty = crate::event_type(fs[4]);
                            let pr = if ty == 2 { crate::parse_price(fs[6]).unwrap_or(7) } else { 0 };
                            acc = acc.wrapping_add(k[0] ^ k[1] ^ u[0] ^ u[1] ^ ts as u64 ^ ing.0 as u64 ^ ty as u64 ^ pr as u64);
                        }),
                    }
                }
                std::hint::black_box(acc);
                lines.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
            });
            let per = w * 1e9 * threads as f64 / lines.load(std::sync::atomic::Ordering::Relaxed).max(1) as f64;
            dl!("[diag] micro kind {kind} threads {threads}: {per:.1} ns/line");
            s.push_str(&format!("{}{threads}:{per:.1};", ["o", "l", "O", "L"][kind]));
        }
    }
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
    let t0 = Instant::now();
    let scratch = std::path::PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    let mut row = String::from("v4;");
    let syn = scratch.join("syn");
    let bytes = synthesize(data, &syn, 20, nt);
    dl!("[diag] synthetic x20 data: {} MB in {:.0} ms", bytes >> 20, t0.elapsed().as_secs_f64() * 1e3);
    let out = scratch.join("synout");

    // pipeline variants: code:wall,link map,phase 1,dedup,phase 3,user,sys (best of 3)
    let sets: Vec<(&str, Vec<(&str, &str)>)> = vec![("d", vec![]), ("t0", vec![("SS_THP", "0")]), ("t1", vec![("SS_THP", "1")]), ("d2", vec![])];
    for (name, envs) in &sets {
        let mut best = (f64::MAX, [0f64; 6]);
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
        let rec = format!("{name}:{:.0},{:.0},{:.0},{:.0},{:.0},{:.1},{:.1};", best.0, l[0], l[1] - l[0], l[2] - l[1], l[3] - l[2], l[4], l[5]);
        dl!("[diag] variant {rec}");
        row.push_str(&rec);
    }
    row.push(';');
    row.push_str(&micro(data, nt));
    row.push(';');

    // profile of the default pipeline: several runs, samples summed by table key
    let table = load_table();
    let prof = scratch.join("prof.txt");
    let prof_s = prof.to_string_lossy().to_string();
    let mut counts: HashMap<String, u64> = HashMap::new();
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
    let names: Vec<String> = v.iter().filter(|(k, _)| k.parse::<u64>().is_err()).take(10).map(|(k, c)| format!("{k}={c}")).collect();
    row.push_str(&names.join(","));
    row.push(';');
    for (k, c) in v.iter() {
        if let Ok(code) = k.parse::<u64>() {
            if row.len() + 6 > 1750 {
                break;
            }
            row.push_str(&b36(code, 3));
            row.push_str(&b36(*c, 3));
        }
    }
    dl!("[diag] done in {:.0} ms: {} samples in {runs} runs, {} keys, row {} chars", t0.elapsed().as_secs_f64() * 1e3, total, v.len(), row.len());
    dl!("[diag] {row}");
    *ROW.lock().unwrap() = row;
}
