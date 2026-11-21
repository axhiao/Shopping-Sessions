use super::*;

fn padded(v: &[u8]) -> Vec<u8> {
    let mut b = v.to_vec();
    b.extend_from_slice(&[b'7'; 40]); // junk after the field must not matter
    b
}

#[test]
fn fast_ts_matches_reference() {
    let mut seeds: Vec<Vec<u8>> = vec![
        b"2026-10-03T23:58:10Z".to_vec(),
        b"2026-10-03T19:58:10-04:00".to_vec(),
        b"2028-02-29T00:00:00Z".to_vec(),
        b"2027-02-29T00:00:00Z".to_vec(),
        b"2026-10-04T01:30:00+02:00".to_vec(),
        b"2026-10-04T01:30:00+23:59".to_vec(),
        b"2026-10-04T01:30:00-00:00".to_vec(),
        b"2026-10-04T01:30:00+24:00".to_vec(),
        b"2026-10-04T01:30:00+05:60".to_vec(),
        b"2026-10-04T01:30:00+0200".to_vec(),
        b"1791071890000".to_vec(),
        b"0001791071890000".to_vec(),
        b"86400000".to_vec(),
        b"0".to_vec(),
        b"123456789".to_vec(),
        b"1791071890000.0".to_vec(),
        b"1.79e12".to_vec(),
        b"-5".to_vec(),
        b"2026-10-03 23:58:10Z".to_vec(),
        b"2026-10-03T23:58:10z".to_vec(),
        b"2026-10-03t23:58:10Z".to_vec(),
        b"2026-13-03T23:58:10Z".to_vec(),
        b"2026-10-03T24:00:00Z".to_vec(),
        b"2026-10-03T23:60:00Z".to_vec(),
        b"2026-10-03T23:59:60Z".to_vec(),
        b"2026-10-03T23:58:10.123Z".to_vec(),
        b"2026-10-03T23:58:10".to_vec(),
        b"123456789012345678901".to_vec(),
        "１７９１０７１８９００００".as_bytes().to_vec(),
    ];
    // mutate every byte of a few valid forms through interesting values
    let bases: Vec<Vec<u8>> = seeds.iter().take(4).cloned().collect();
    for base in &bases {
        for i in 0..base.len() {
            for c in [b'0', b'9', b'/', b':', b'-', b'+', b'T', b'Z', b' ', b'a', 0x80, b'5', b'2', b'1', b'3'] {
                let mut v = base.clone();
                v[i] = c;
                seeds.push(v);
            }
        }
    }
    let mut x: u64 = 12345;
    for _ in 0..200_000 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let n = (x >> 59) as usize % 18 + 1;
        let v: Vec<u8> = (0..n).map(|k| b'0' + ((x >> (k * 3)) % 10) as u8).collect();
        seeds.push(v);
        let y = 2000 + (x >> 20) % 40;
        let mo = (x >> 30) % 14;
        let d = (x >> 34) % 33;
        let h = (x >> 40) % 26;
        let mi = (x >> 46) % 62;
        let se = (x >> 52) % 62;
        let iso = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}");
        seeds.push(format!("{iso}Z").into_bytes());
        let oh = (x >> 8) % 26;
        let om = (x >> 14) % 62;
        let sg = if x & 1 == 0 { '+' } else { '-' };
        seeds.push(format!("{iso}{sg}{oh:02}:{om:02}").into_bytes());
    }
    let mut dc = DateCache::new();
    for v in &seeds {
        let p = padded(v);
        let f = &p[..v.len()];
        assert_eq!(parse_ts_fast(f), parse_ts(f), "{:?}", String::from_utf8_lossy(v));
        assert_eq!(parse_ts_dc(&mut dc, f), parse_ts(f), "dc {:?}", String::from_utf8_lossy(v));
    }
    // again in another order (other cache states), and with other junk after the field
    for v in seeds.iter().rev() {
        let mut p = v.to_vec();
        p.extend_from_slice(b"Z:+-T0123456789,\"\n00000000000000000000000");
        let f = &p[..v.len()];
        assert_eq!(parse_ts_dc(&mut dc, f), parse_ts(f), "dc rev {:?}", String::from_utf8_lossy(v));
    }
}

#[test]
fn key_of_matches_reference() {
    let mut x: u64 = 77;
    for _ in 0..200_000 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let n = (x >> 60) as usize; // 0..=15
        let mut b = [0u8; 48];
        for (i, c) in b.iter_mut().enumerate() {
            *c = (x.rotate_left(i as u32 * 7) >> 11) as u8 | 1;
        }
        let s = &b[..n];
        let raw = u128::from_be_bytes(b[..16].try_into().unwrap());
        let mask = if n == 0 { 0 } else { !0u128 << (128 - 8 * n) };
        let v = (raw & mask) | n as u128;
        assert_eq!(key_of(s), [(v >> 64) as u64, v as u64], "n {n}");
    }
}

#[test]
#[ignore]
fn bench_ts() {
    let mut buf = Vec::new();
    let mut offs = Vec::new();
    let mut x: u64 = 99;
    for _ in 0..1_000_000 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let d = 9 + (x >> 40) % 14;
        let h = (x >> 20) % 24;
        let mi = (x >> 30) % 60;
        let se = (x >> 50) % 60;
        offs.push(buf.len());
        buf.extend_from_slice(format!("2027-12-{d:02}T{h:02}:{mi:02}:{se:02}Z,").as_bytes());
    }
    buf.extend_from_slice(&[0u8; 64]);
    for round in 0..3 {
        let t = std::time::Instant::now();
        let mut acc = 0i64;
        for _ in 0..10 {
            for &o in &offs {
                acc = acc.wrapping_add(parse_ts_fast(&buf[o..o + 20]).unwrap_or(0));
            }
        }
        let e = t.elapsed().as_secs_f64();
        let t2 = std::time::Instant::now();
        let mut acc2 = 0i64;
        for _ in 0..10 {
            for &o in &offs {
                acc2 = acc2.wrapping_add(parse_ts(&buf[o..o + 20]).unwrap_or(0));
            }
        }
        let e2 = t2.elapsed().as_secs_f64();
        eprintln!("round {round}: fast {:.2} ns/ts, old {:.2} ns/ts ({acc} {acc2})", e * 1e9 / 1e7, e2 * 1e9 / 1e7);
    }
}

#[test]
fn days_fast_matches() {
    for y in 0..=9999u32 {
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        for m in 1..=12u32 {
            for d in [1u32, 15, 28] {
                assert_eq!(days_fast(y, m, d, leap), days_from_civil(y as i64, m as i64, d as i64), "{y}-{m}-{d}");
            }
        }
    }
}

#[test]
fn ingest_matches_on_valid_f1() {
    let mut dc = DateCache::new();
    let mut x: u64 = 7;
    for _ in 0..300_000 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let y = 1970 + (x >> 20) % 80;
        let mo = 1 + (x >> 30) % 12;
        let d = 1 + (x >> 34) % 28;
        let (h, mi, se) = ((x >> 40) % 24, (x >> 46) % 60, (x >> 52) % 60);
        let v = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z").into_bytes();
        let p = padded(&v);
        assert_eq!(parse_ingest(&p[..20]), parse_ts(&p[..20]));
        assert_eq!(parse_ingest_dc(&mut dc, &p[..20]).map(|v| v.1), parse_ts(&p[..20]));
        assert_eq!(parse_ingest_dc(&mut dc, &p[..20]).map(|v| v.0 * 1000), parse_ts(&p[..20]));
    }
    for v in [&b"2028-02-29T23:59:59Z"[..], b"2027-12-31T23:59:59Z", b"2026-10-03T23:58:10Z"] {
        let p = padded(v);
        assert_eq!(parse_ingest(&p[..20]), parse_ts(&p[..20]));
    }
}

#[test]
fn year_table_matches() {
    for y in 0..=9999u32 {
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        let (b, l) = year_info(y);
        assert_eq!(l, leap);
        for m in 1..=12u32 {
            assert_eq!(days_from_parts(b, l, m, 1), days_fast(y, m, 1, leap));
        }
    }
}

/// Random CSV text with every kind of dirt the dialect knows.
fn random_csv(x: &mut u64, lines: usize, nf: usize) -> Vec<u8> {
    let mut rnd = |m: u64| {
        *x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*x >> 33) % m
    };
    let mut out = b"h1,h2\n".to_vec();
    for _ in 0..lines {
        let kind = rnd(100);
        if kind == 0 {
            // empty line
        } else if kind == 1 {
            // a very long line of separators or text
            let n = rnd(3000) as usize;
            let c = [b',', b'x', b'"'][rnd(3) as usize];
            out.extend(std::iter::repeat(c).take(n));
        } else {
            let count = match rnd(40) {
                0 => nf + 1,
                1 => nf - 1,
                2 => rnd(12) as usize + 1,
                _ => nf,
            };
            for i in 0..count {
                if i > 0 {
                    out.push(b',');
                }
                let len = match rnd(10) {
                    0 => 0,
                    1 => 1,
                    2 => rnd(70) as usize,
                    _ => rnd(12) as usize,
                };
                let quoted = rnd(5) == 0;
                if quoted {
                    out.push(b'"');
                }
                for _ in 0..len {
                    let c = match rnd(60) {
                        0 => b'"',
                        1 => b'\r',
                        2 => b' ',
                        3 => 0xC3,
                        v => b'a' + (v % 26) as u8,
                    };
                    out.push(c);
                }
                if quoted && rnd(50) != 0 {
                    out.push(b'"');
                }
                if rnd(80) == 0 {
                    out.push(b'"');
                }
            }
        }
        out.push(b'\n');
    }
    out
}

fn collect<const N: usize, const K: usize>(fast: bool, buf: &[u8], begin: usize, limit: usize) -> Vec<Vec<Vec<u8>>> {
    let mut rows = Vec::new();
    let mut f = |fs: &[&[u8]; N]| rows.push(fs.iter().map(|v| v.to_vec()).collect::<Vec<_>>());
    if fast {
        scan_lean::<N, K, _>(buf, begin, limit, &mut f);
    } else {
        scan_lines::<N, _>(buf, begin, limit, &mut f);
    }
    rows
}

#[test]
fn fast_scanner_matches_reference() {
    let mut x: u64 = 0x5EED;
    for round in 0..600 {
        let lines = [1usize, 3, 40, 400, 3000][round % 5];
        for nf in [2usize, 4, 7] {
            let text = random_csv(&mut x, lines, nf);
            let buf = pad_buffer(text);
            let n = buf.len();
            // every line start is a possible begin; a few limits
            let starts: Vec<usize> = (0..n).filter(|&i| buf[i] == b'\n').map(|i| i + 1).collect();
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let begin = starts[(x >> 33) as usize % starts.len().min(8)];
            for limit in [n, begin + 1, begin + (n - begin) / 2, begin + (n - begin) / 3 + 1] {
                // the buffer handed to a scanner ends with the first '\n' at or after limit - 1
                let stop = if limit >= n { n } else { find_byte(&buf, limit - 1, b'\n') + 1 };
                let b = &buf[..stop.min(n)];
                macro_rules! check {
                    ($n:expr, $k:expr) => {
                        assert_eq!(collect::<$n, $k>(true, b, begin, limit), collect::<$n, $k>(false, b, begin, limit), "round {round} nf {nf} begin {begin} limit {limit}")
                    };
                }
                match nf {
                    2 => check!(2, 1),
                    4 => {
                        check!(4, 1);
                        check!(4, 2);
                        check!(4, 3)
                    }
                    _ => {
                        check!(7, 4);
                        check!(7, 3);
                        check!(7, 6);
                        check!(7, 1)
                    }
                }
            }
        }
    }
}

/// Hand-picked quote cases: the fast scanner agrees with the plain one.
#[test]
fn fast_scanner_quote_cases() {
    let cases: [&[u8]; 16] = [
        b"a,b", b"\"a\",b", b"a,\"b\"", b"\"\",\"\"", b"\",b", b"a,\"", b"\"a,b", b"a\",b", b"a,b\"", b"\"a\"b,c",
        b"\"a\"\",c", b"\"\"a,c", b"\"\"\",c", b"\"\"\"\",c", b"a\"\"b,c", b"a,b,c",
    ];
    // all pairs of cases on consecutive lines, at many alignments
    for pad in 0..70 {
        for a in cases {
            for b in cases {
                let mut text = b"h\n".to_vec();
                text.extend(std::iter::repeat(b'x').take(pad));
                text.extend_from_slice(b",y\n");
                text.extend_from_slice(a);
                text.push(b'\n');
                text.extend_from_slice(b);
                text.push(b'\n');
                let buf = pad_buffer(text);
                assert_eq!(collect::<2, 1>(true, &buf, 2, buf.len()), collect::<2, 1>(false, &buf, 2, buf.len()), "{:?}", String::from_utf8_lossy(&buf));
            }
        }
    }
}

#[test]
#[ignore]
fn bench_scan() {
    let path = std::env::var("SS_BENCH_FILE").expect("SS_BENCH_FILE");
    let mut text = std::fs::read(path).unwrap();
    text.truncate(text.len().min(1 << 20));
    while text.last() != Some(&b'\n') {
        text.pop();
    }
    let buf = pad_buffer(text);
    let begin = find_byte(&buf, 0, b'\n') + 1;
    for round in 0..3 {
        for fast in [false, true] {
            let t = std::time::Instant::now();
            let (mut lines, mut acc) = (0u64, 0u64);
            for _ in 0..200 {
                let mut f = |fs: &[&[u8]; 7]| {
                    lines += 1;
                    acc = acc.wrapping_add(fs[0].len() as u64 + fs[3].as_ptr() as u64 + fs[6].len() as u64);
                };
                if fast {
                    scan_fast::<7, _>(&buf, begin, buf.len(), &mut f);
                } else {
                    scan_lines::<7, _>(&buf, begin, buf.len(), &mut f);
                }
            }
            let e = t.elapsed().as_secs_f64();
            eprintln!("round {round} fast={fast}: {:.2} ns/line, {:.2} GB/s ({lines} {acc})", e * 1e9 / lines as f64, buf.len() as f64 * 200.0 / e / 1e9);
        }
    }
}

#[test]
#[ignore]
fn bench_scan2() {
    let path = std::env::var("SS_BENCH_FILE").expect("SS_BENCH_FILE");
    let mut text = std::fs::read(path).unwrap();
    text.truncate(text.len().min(1 << 20));
    while text.last() != Some(&b'\n') {
        text.pop();
    }
    let buf = pad_buffer(text);
    let begin = find_byte(&buf, 0, b'\n') + 1;
    let mut dc = DateCache::new();
    for mode in 0..4 {
        for fast in [0, 1, 2] {
            let t = std::time::Instant::now();
            let (mut lines, mut acc) = (0u64, 0u64);
            for _ in 0..200 {
                let mut f = |fs: &[&[u8]; 7]| {
                    lines += 1;
                    match mode {
                        0 => acc = acc.wrapping_add(fs[0].len() as u64 + fs[3].as_ptr() as u64 + fs[6].len() as u64),
                        1 => {
                            for x in fs {
                                acc = acc.wrapping_add(x.len() as u64 ^ x.as_ptr() as u64);
                            }
                        }
                        3 => {
                            let k = key_of(fs[0]);
                            let u = key_of(fs[1]);
                            let ts = parse_ts_dc(&mut dc, fs[2]).unwrap_or(1);
                            let ing = parse_ingest_dc(&mut dc, fs[3]).unwrap_or((2, 2));
                            let ty = event_type(fs[4]);
                            let pr = if ty == 2 { parse_price(fs[6]).unwrap_or(7) } else { 0 };
                            let p = key_of(fs[5]);
                            acc = acc.wrapping_add(k[0] ^ k[1] ^ u[0] ^ u[1] ^ ts as u64 ^ ing.0 as u64 ^ ing.1 as u64 ^ ty as u64 ^ pr as u64 ^ p[0] ^ p[1]);
                        }
                        _ => {
                            let k = key_of(fs[0]);
                            let u = key_of(fs[1]);
                            let ts = parse_ts_fast(fs[2]).unwrap_or(1);
                            let ing = parse_ingest_s(fs[3]).unwrap_or((2, 2));
                            let ty = event_type(fs[4]);
                            let pr = if ty == 2 { parse_price(fs[6]).unwrap_or(7) } else { 0 };
                            let p = key_of(fs[5]);
                            acc = acc.wrapping_add(k[0] ^ k[1] ^ u[0] ^ u[1] ^ ts as u64 ^ ing.0 as u64 ^ ing.1 as u64 ^ ty as u64 ^ pr as u64 ^ p[0] ^ p[1]);
                        }
                    }
                };
                match fast {
                    0 => scan_lines::<7, _>(&buf, begin, buf.len(), &mut f),
                    1 => scan_fast::<7, _>(&buf, begin, buf.len(), &mut f),
                    _ => scan_lean::<7, 3, _>(&buf, begin, buf.len(), &mut f),
                }
            }
            let e = t.elapsed().as_secs_f64();
            eprintln!("mode {mode} scanner={fast}: {:.2} ns/line ({lines} {acc})", e * 1e9 / lines as f64);
        }
    }
}

#[inline(never)]
pub fn probe_lean(buf: &[u8], begin: usize) -> u64 {
    let mut acc = 0u64;
    scan_lean::<7, 3, _>(buf, begin, buf.len(), |fs| {
        for x in fs {
            acc = acc.wrapping_add(x.len() as u64 ^ x.as_ptr() as u64);
        }
    });
    acc
}

#[test]
#[ignore]
fn bench_probe() {
    let path = std::env::var("SS_BENCH_FILE").expect("SS_BENCH_FILE");
    let mut text = std::fs::read(path).unwrap();
    text.truncate(text.len().min(1 << 20));
    while text.last() != Some(&b'\n') {
        text.pop();
    }
    let buf = pad_buffer(text);
    let begin = find_byte(&buf, 0, b'\n') + 1;
    let lines = buf.iter().filter(|&&c| c == b'\n').count() - 1;
    for _ in 0..3 {
        let t = std::time::Instant::now();
        let mut acc = 0;
        for _ in 0..200 {
            acc ^= probe_lean(std::hint::black_box(&buf), begin);
        }
        eprintln!("probe: {:.2} ns/line {acc}", t.elapsed().as_secs_f64() * 1e9 / 200.0 / lines as f64);
    }
}
