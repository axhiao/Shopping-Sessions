//! Minimal SIGPROF sampling profiler (enabled with SS_PROF=path): records the
//! interrupted instruction pointer; `dump` writes "offset count" lines.

use std::sync::atomic::{AtomicUsize, Ordering};

const MAX: usize = 1 << 20;
static mut SAMPLES: [usize; MAX] = [0; MAX];
static N: AtomicUsize = AtomicUsize::new(0);

#[repr(C)]
struct SigAction {
    handler: usize,
    mask: [u64; 16],
    flags: i32,
    restorer: usize,
}

#[repr(C)]
struct TimeVal {
    sec: i64,
    usec: i64,
}

#[repr(C)]
struct ITimerVal {
    interval: TimeVal,
    value: TimeVal,
}

extern "C" {
    fn sigaction(sig: i32, act: *const SigAction, old: *mut SigAction) -> i32;
    fn setitimer(which: i32, new: *const ITimerVal, old: *mut ITimerVal) -> i32;
}

extern "C" fn on_prof(_sig: i32, _info: *mut u8, ctx: *mut u8) {
    unsafe {
        let rip = *(ctx.add(168) as *const usize);
        let i = N.fetch_add(1, Ordering::Relaxed);
        if i < MAX {
            *(std::ptr::addr_of_mut!(SAMPLES) as *mut usize).add(i) = rip;
        }
    }
}

pub fn start(us: i64) {
    unsafe {
        let act = SigAction { handler: on_prof as *const () as usize, mask: [0; 16], flags: 4 | 0x1000_0000, restorer: 0 };
        sigaction(27, &act, std::ptr::null_mut());
        let t = ITimerVal { interval: TimeVal { sec: 0, usec: us }, value: TimeVal { sec: 0, usec: us } };
        setitimer(2, &t, std::ptr::null_mut());
    }
}

pub fn dump(path: &str) {
    unsafe {
        let t = ITimerVal { interval: TimeVal { sec: 0, usec: 0 }, value: TimeVal { sec: 0, usec: 0 } };
        setitimer(2, &t, std::ptr::null_mut());
    }
    let exe = std::fs::read_link("/proc/self/exe").unwrap_or_default();
    let maps = std::fs::read_to_string("/proc/self/maps").unwrap_or_default();
    let mut base = 0usize;
    for l in maps.lines() {
        if l.ends_with(exe.to_str().unwrap_or("?")) {
            let mut it = l.split_whitespace();
            let range = it.next().unwrap();
            let _perm = it.next();
            let off = usize::from_str_radix(it.next().unwrap(), 16).unwrap();
            if off == 0 {
                base = usize::from_str_radix(range.split('-').next().unwrap(), 16).unwrap();
                break;
            }
        }
    }
    let n = N.load(Ordering::Relaxed).min(MAX);
    let mut counts = std::collections::HashMap::new();
    for i in 0..n {
        let r = unsafe { *(std::ptr::addr_of!(SAMPLES) as *const usize).add(i) };
        *counts.entry(r.wrapping_sub(base)).or_insert(0usize) += 1;
    }
    let mut v: Vec<_> = counts.into_iter().collect();
    v.sort_by(|a, b| b.1.cmp(&a.1));
    let mut s = String::new();
    for (a, c) in v {
        s.push_str(&format!("{a:x} {c}\n"));
    }
    let _ = std::fs::write(path, s);
}
