//! Scorer diagnostics (feature "diag"): prints what the machine looks like and a
//! few micro-benchmarks to stderr. Never part of a scored build.

use std::fs;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::Instant;

extern "C" {
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn mincore(addr: *mut u8, len: usize, vec: *mut u8) -> i32;
    fn getrusage(who: i32, usage: *mut [i64; 18]) -> i32;
}

fn cpu() -> (f64, f64) {
    let mut ru = [0i64; 18];
    unsafe { getrusage(0, &mut ru) };
    (ru[0] as f64 + ru[1] as f64 * 1e-6, ru[2] as f64 + ru[3] as f64 * 1e-6)
}

fn cat(path: &str, max_lines: usize) {
    match fs::read_to_string(path) {
        Ok(s) => {
            for l in s.lines().take(max_lines) {
                eprintln!("[diag] {path}: {l}");
            }
        }
        Err(e) => eprintln!("[diag] {path}: <{e}>"),
    }
}

fn grep(path: &str, keys: &[&str], max: usize) {
    if let Ok(s) = fs::read_to_string(path) {
        let mut n = 0;
        for l in s.lines() {
            if keys.iter().any(|k| l.contains(k)) {
                eprintln!("[diag] {path}: {l}");
                n += 1;
                if n >= max {
                    break;
                }
            }
        }
    } else {
        eprintln!("[diag] {path}: <unreadable>");
    }
}

/// Runs `f(thread)` on `nt` threads; returns (wall s, user s, sys s).
fn timed<F: Fn(usize) + Sync>(nt: usize, f: F) -> (f64, f64, f64) {
    let c0 = cpu();
    let t = Instant::now();
    std::thread::scope(|s| {
        for w in 0..nt {
            let f = &f;
            s.spawn(move || f(w));
        }
    });
    let e = t.elapsed().as_secs_f64();
    let c1 = cpu();
    (e, c1.0 - c0.0, c1.1 - c0.1)
}

fn anon(len: usize, thp: bool) -> *mut u8 {
    unsafe {
        let p = mmap(std::ptr::null_mut(), len + (2 << 20), 3, 0x22 | 0x4000, -1, 0);
        let a = ((p as usize + (2 << 20) - 1) & !((2 << 20) - 1)) as *mut u8;
        madvise(a, len, if thp { 14 } else { 15 });
        a
    }
}

fn mem_tests(nt: usize) {
    const SZ: usize = 256 << 20;
    for thp in [false, true] {
        // first-touch cost
        let ptrs: Vec<usize> = (0..nt).map(|_| anon(SZ, thp) as usize).collect();
        let (w, u, s) = timed(nt, |i| unsafe {
            let p = ptrs[i] as *mut u8;
            let mut o = 0;
            while o < SZ {
                p.add(o).write_volatile(1);
                o += 4096;
            }
        });
        let huge = fs::read_to_string("/proc/self/smaps_rollup")
            .ok()
            .and_then(|s| s.lines().find(|l| l.starts_with("AnonHugePages")).map(|l| l.to_string()))
            .unwrap_or_default();
        eprintln!(
            "[diag] fault thp={thp} threads={nt}: {:.0} ms wall, user {:.2} sys {:.2}, {:.2} GB/s, {:.2} us/4K-page cpu  [{huge}]",
            w * 1e3,
            u,
            s,
            (nt * SZ) as f64 / w / 1e9,
            (u + s) * 1e6 / (nt * SZ / 4096) as f64
        );
        if !thp {
            // bandwidth on the faulted 4K memory
            for (name, mode) in [("read", 0), ("write", 1), ("write-nt", 2), ("rep-stosb", 3)] {
                let (w, _, _) = timed(nt, |i| unsafe {
                    use std::arch::x86_64::*;
                    let p = ptrs[i] as *mut u64;
                    let n = SZ / 8;
                    match mode {
                        0 => {
                            let mut a = _mm256_setzero_si256();
                            let mut i = 0;
                            while i < n {
                                a = _mm256_add_epi64(a, _mm256_load_si256(p.add(i) as *const __m256i));
                                i += 4;
                            }
                            std::hint::black_box(a);
                        }
                        1 => {
                            let v = _mm256_set1_epi64x(3);
                            let mut i = 0;
                            while i < n {
                                _mm256_store_si256(p.add(i) as *mut __m256i, v);
                                i += 4;
                            }
                        }
                        2 => {
                            let v = _mm256_set1_epi64x(5);
                            let mut i = 0;
                            while i < n {
                                _mm256_stream_si256(p.add(i) as *mut __m256i, v);
                                i += 4;
                            }
                            _mm_sfence();
                        }
                        _ => {
                            std::arch::asm!("rep stosb", inout("rcx") SZ => _, inout("rdi") p => _, in("al") 0u8);
                        }
                    }
                });
                eprintln!("[diag] bandwidth {name} threads={nt}: {:.1} GB/s", (nt * SZ) as f64 / w / 1e9);
            }
            // random 64-byte reads in the 256 MB block, 8 independent chains
            let (w, _, _) = timed(nt, |i| unsafe {
                let p = ptrs[i] as *const u64;
                let mut x = [1u64 + i as u64, 2, 3, 4, 5, 6, 7, 8];
                let mut acc = 0u64;
                for _ in 0..500_000 {
                    for k in 0..8 {
                        x[k] = x[k].wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407 + k as u64);
                        acc = acc.wrapping_add(*p.add(((x[k] >> 20) as usize % (SZ / 64)) * 8));
                    }
                }
                std::hint::black_box(acc);
            });
            eprintln!("[diag] random read (8 chains) threads={nt}: {:.1} ns/read", w * 1e9 / 4e6);
        } else {
            let (w, _, _) = timed(nt, |i| unsafe {
                let p = ptrs[i] as *const u64;
                let mut x = [1u64 + i as u64, 2, 3, 4, 5, 6, 7, 8];
                let mut acc = 0u64;
                for _ in 0..500_000 {
                    for k in 0..8 {
                        x[k] = x[k].wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407 + k as u64);
                        acc = acc.wrapping_add(*p.add(((x[k] >> 20) as usize % (SZ / 64)) * 8));
                    }
                }
                std::hint::black_box(acc);
            });
            eprintln!("[diag] random read thp (8 chains) threads={nt}: {:.1} ns/read", w * 1e9 / 4e6);
        }
        let (w, u, s) = timed(nt, |i| unsafe {
            madvise(ptrs[i] as *mut u8, SZ, 4);
        });
        eprintln!("[diag] dontneed thp={thp} threads={nt}: {:.0} ms wall, cpu {:.2}", w * 1e3, u + s);
        for p in ptrs {
            unsafe { munmap(p as *mut u8, SZ) };
        }
    }
}

fn io_tests(data: &Path, out: &Path, scratch: &Path, nt: usize) {
    use std::os::unix::io::AsRawFd;
    let mut files: Vec<(std::path::PathBuf, u64)> = Vec::new();
    for dir in ["events", "clicks", "links"] {
        let mut n = 0u64;
        let mut bytes = 0u64;
        if let Ok(rd) = fs::read_dir(data.join(dir)) {
            for e in rd.flatten() {
                if let Ok(m) = e.metadata() {
                    if m.is_file() {
                        n += 1;
                        bytes += m.len();
                        files.push((e.path(), m.len()));
                    }
                }
            }
        }
        eprintln!("[diag] {dir}: {n} files, {bytes} bytes");
    }
    files.sort();
    let total: u64 = files.iter().map(|f| f.1).sum();
    // how much of the input is already in the page cache?
    let mut resident = 0u64;
    let mut pages = 0u64;
    let t = Instant::now();
    for (p, len) in &files {
        if *len == 0 {
            continue;
        }
        let f = fs::File::open(p).unwrap();
        unsafe {
            let m = mmap(std::ptr::null_mut(), *len as usize, 1, 0x02, f.as_raw_fd(), 0);
            if m as isize == -1 {
                continue;
            }
            let np = (*len as usize).div_ceil(4096);
            let mut v = vec![0u8; np];
            if mincore(m, *len as usize, v.as_mut_ptr()) == 0 {
                resident += v.iter().filter(|&&b| b & 1 != 0).count() as u64;
                pages += np as u64;
            }
            munmap(m, *len as usize);
        }
    }
    eprintln!("[diag] page cache before reading: {resident} of {pages} pages resident ({:.1} ms to check)", t.elapsed().as_secs_f64() * 1e3);
    let next = std::sync::atomic::AtomicUsize::new(0);
    for pass in 0..2 {
        next.store(0, std::sync::atomic::Ordering::Relaxed);
        let (w, u, s) = timed(nt, |_| {
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= files.len() {
                    break;
                }
                let f = fs::File::open(&files[i].0).unwrap();
                let mut off = 0u64;
                loop {
                    let n = f.read_at(&mut buf, off).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    off += n as u64;
                }
            }
        });
        eprintln!("[diag] pread pass {pass} threads={nt}: {:.1} ms, {:.2} GB/s, user {:.2} sys {:.2}", w * 1e3, total as f64 / w / 1e9, u, s);
    }
    next.store(0, std::sync::atomic::Ordering::Relaxed);
    let (w, u, s) = timed(nt, |_| loop {
        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if i >= files.len() {
            break;
        }
        let len = files[i].1 as usize;
        if len == 0 {
            continue;
        }
        let f = fs::File::open(&files[i].0).unwrap();
        unsafe {
            let m = mmap(std::ptr::null_mut(), len, 1, 0x02, f.as_raw_fd(), 0);
            madvise(m, len, 22);
            let mut acc = 0u64;
            let mut o = 0;
            while o < len {
                acc += *m.add(o) as u64;
                o += 64;
            }
            std::hint::black_box(acc);
            madvise(m, len, 4);
            munmap(m, len);
        }
    });
    eprintln!("[diag] mmap+populate+touch+release threads={nt}: {:.1} ms, {:.2} GB/s, user {:.2} sys {:.2}", w * 1e3, total as f64 / w / 1e9, u, s);
    for (name, dir) in [("output", out), ("scratch", scratch)] {
        let _ = fs::create_dir_all(dir);
        let (w, u, s) = timed(nt, |i| {
            let p = dir.join(format!("_diag{i}.tmp"));
            let buf = vec![7u8; 4 << 20];
            if let Ok(f) = fs::File::create(&p) {
                for k in 0..8u64 {
                    let _ = f.write_all_at(&buf, k * (4 << 20));
                }
            }
        });
        let ok = dir.join("_diag0.tmp").exists();
        let t = Instant::now();
        for i in 0..nt {
            let _ = fs::remove_file(dir.join(format!("_diag{i}.tmp")));
        }
        eprintln!(
            "[diag] write {name} ({}x32 MB, ok={ok}): {:.1} ms, {:.2} GB/s, user {:.2} sys {:.2}; unlink {:.1} ms",
            nt,
            w * 1e3,
            (nt * (32 << 20)) as f64 / w / 1e9,
            u,
            s,
            t.elapsed().as_secs_f64() * 1e3
        );
    }
    // scanner micro-benchmark on the first megabyte of the biggest events file
    let big = files.iter().filter(|f| f.0.to_string_lossy().contains("/events/")).max_by_key(|f| f.1);
    if let Some((p, _)) = big {
        let mut text = fs::read(p).unwrap_or_default();
        text.truncate(text.len().min(1 << 20));
        while !text.is_empty() && text.last() != Some(&b'\n') {
            text.pop();
        }
        if text.len() > 1000 {
            let buf = crate::pad_buffer(text);
            let begin = crate::find_byte(&buf, 0, b'\n') + 1;
            for threads in [1, nt] {
                for fast in [false, true] {
                    let lines = std::sync::atomic::AtomicU64::new(0);
                    let (w, _, _) = timed(threads, |_| {
                        let (mut n, mut acc) = (0u64, 0u64);
                        for _ in 0..60 {
                            let mut f = |fs: &[&[u8]; 7]| {
                                n += 1;
                                acc = acc.wrapping_add(fs[0].len() as u64 + fs[3].as_ptr() as u64 + fs[6].len() as u64);
                            };
                            if fast {
                                crate::scan_fast::<7, _>(&buf, begin, buf.len(), &mut f);
                            } else {
                                crate::scan_lines::<7, _>(&buf, begin, buf.len(), &mut f);
                            }
                        }
                        std::hint::black_box(acc);
                        lines.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
                    });
                    let n = lines.load(std::sync::atomic::Ordering::Relaxed);
                    eprintln!("[diag] scan fast={fast} threads={threads}: {:.2} ns/line/thread", w * 1e9 * threads as f64 / n as f64);
                }
            }
        }
    }
}

pub fn run(data: &Path, out: &Path, nt: usize) {
    let t0 = Instant::now();
    eprintln!("[diag] ---- machine");
    grep("/proc/cpuinfo", &["model name", "cpu MHz", "cache size", "siblings", "cpu cores"], 6);
    if let Ok(s) = fs::read_to_string("/proc/cpuinfo") {
        eprintln!("[diag] processors listed: {}", s.lines().filter(|l| l.starts_with("processor")).count());
        if let Some(l) = s.lines().find(|l| l.starts_with("flags")) {
            let want = [
                "avx2", "bmi2", "avx512f", "avx512bw", "avx512vl", "avx512dq", "avx512_vbmi", "avx512_vbmi2", "avx512_bitalg",
                "avx512_vpopcntdq", "vpclmulqdq", "gfni", "vaes", "erms", "fsrm", "hypervisor", "pdpe1gb", "movdir64b",
            ];
            let have: Vec<&str> = l.split_whitespace().filter(|f| want.contains(f)).collect();
            eprintln!("[diag] flags of interest: {}", have.join(" "));
        }
    }
    grep("/proc/self/status", &["Cpus_allowed_list", "Mems_allowed_list", "Threads"], 4);
    if let Ok(s) = fs::read_to_string("/proc/self/status") {
        if let Some(l) = s.lines().find(|l| l.starts_with("Cpus_allowed_list")) {
            let list = l.split(':').nth(1).unwrap_or("").trim();
            for part in list.split(',') {
                let mut it = part.split('-');
                let a: usize = it.next().and_then(|v| v.parse().ok()).unwrap_or(0);
                let b: usize = it.next().and_then(|v| v.parse().ok()).unwrap_or(a);
                for c in a..=b.min(a + 15) {
                    let r = |f: &str| fs::read_to_string(format!("/sys/devices/system/cpu/cpu{c}/{f}")).unwrap_or_default().trim().to_string();
                    eprintln!(
                        "[diag] cpu{c}: siblings {} core {} pkg {} maxfreq {} cur {}",
                        r("topology/thread_siblings_list"),
                        r("topology/core_id"),
                        r("topology/physical_package_id"),
                        r("cpufreq/cpuinfo_max_freq"),
                        r("cpufreq/scaling_cur_freq")
                    );
                }
            }
        }
    }
    for i in 0..4 {
        let r = |f: &str| fs::read_to_string(format!("/sys/devices/system/cpu/cpu0/cache/index{i}/{f}")).unwrap_or_default().trim().to_string();
        eprintln!("[diag] cache index{i}: level {} type {} size {} shared {}", r("level"), r("type"), r("size"), r("shared_cpu_list"));
    }
    cat("/proc/version", 1);
    grep("/proc/meminfo", &["MemTotal", "MemFree", "MemAvailable", "Cached", "Dirty", "AnonHugePages", "HugePages_Total", "Hugepagesize"], 10);
    for f in [
        "/sys/kernel/mm/transparent_hugepage/enabled",
        "/sys/kernel/mm/transparent_hugepage/defrag",
        "/sys/kernel/mm/transparent_hugepage/shmem_enabled",
        "/proc/sys/vm/overcommit_memory",
        "/proc/sys/vm/dirty_ratio",
        "/proc/sys/vm/dirty_background_ratio",
        "/proc/sys/vm/max_map_count",
        "/proc/sys/kernel/perf_event_paranoid",
        "/proc/self/cgroup",
        "/sys/fs/cgroup/cpu.max",
        "/sys/fs/cgroup/cpuset.cpus.effective",
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory.high",
        "/sys/fs/cgroup/cpu.weight",
        "/sys/devices/system/cpu/vulnerabilities/spectre_v2",
        "/sys/devices/system/cpu/vulnerabilities/mds",
        "/sys/devices/system/cpu/vulnerabilities/meltdown",
        "/proc/loadavg",
    ] {
        cat(f, 3);
    }
    grep("/proc/self/mountinfo", &[" /data ", " /output ", " /scratch ", " /tmp ", " / "], 8);
    for k in ["NCPU", "MEM_GB", "DATA_DIR", "OUTPUT_DIR", "SCRATCH_DIR", "HOME"] {
        eprintln!("[diag] env {k}={:?}", std::env::var(k).ok());
    }
    eprintln!("[diag] ---- io ({:.0} ms)", t0.elapsed().as_secs_f64() * 1e3);
    let scratch = std::path::PathBuf::from(std::env::var("SCRATCH_DIR").unwrap_or_else(|_| "/scratch".into()));
    io_tests(data, out, &scratch, nt);
    eprintln!("[diag] ---- memory ({:.0} ms)", t0.elapsed().as_secs_f64() * 1e3);
    mem_tests(1);
    mem_tests(nt);
    eprintln!("[diag] ---- done ({:.0} ms)", t0.elapsed().as_secs_f64() * 1e3);
}
