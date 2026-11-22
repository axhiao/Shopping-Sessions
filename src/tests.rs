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

/// The digits the generator prints for a number.
fn base36_text(mut v: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        out.push(b"0123456789abcdefghijklmnopqrstuvwxyz"[(v % 36) as usize]);
        v /= 36;
        if v == 0 {
            break;
        }
    }
    out.reverse();
    out
}

/// What `base36_40` must return, the slow way.
fn base36_reference(s: &[u8]) -> Option<u64> {
    if s.is_empty() || s.len() > 8 || (s[0] == b'0' && s.len() > 1) {
        return None;
    }
    let mut v = 0u64;
    for &c in s {
        let d = match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'z' => c - b'a' + 10,
            _ => return None,
        };
        v = v * 36 + d as u64;
    }
    (v <= M40).then_some(v)
}

fn rng(x: &mut u64) -> u64 {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    *x
}

#[test]
fn base36_matches_reference() {
    let mut x = 0x9E37_79B9_7F4A_7C15u64;
    let alphabet: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzAZ_-/:`{ \xff\x00\"";
    let mut cases: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"0".to_vec(),
        b"00".to_vec(),
        b"01".to_vec(),
        b"z".to_vec(),
        base36_text(M40),
        base36_text(M40 + 1),
        b"zzzzzzzz".to_vec(),
        b"zzzzzzzzz".to_vec(),
        b"10000000".to_vec(),
        b"100000000".to_vec(),
    ];
    for _ in 0..200_000 {
        let r = rng(&mut x);
        if r % 3 == 0 {
            cases.push(base36_text(rng(&mut x) % (M40 + 1000)));
        } else {
            let n = (r >> 8) % 11;
            cases.push((0..n).map(|_| alphabet[(rng(&mut x) % alphabet.len() as u64) as usize]).collect());
        }
    }
    let mut accepted = 0;
    for c in &cases {
        let b = padded(c);
        let got = base36_40(&b[..c.len()]);
        assert_eq!(got, base36_reference(c), "{:?}", String::from_utf8_lossy(c));
        if let Some(v) = got {
            accepted += 1;
            // exact: the number gives the same text back
            assert_eq!(&base36_text(v), c);
            let mut id = vec![b'e'];
            id.extend_from_slice(c);
            let pid = padded(&id);
            assert_eq!(id_number(b'e', &pid[..id.len()]), Some(v));
            assert_eq!(id_number(b'k', &pid[..id.len()]), None);
        }
    }
    assert!(accepted > 50_000);
}

#[test]
fn scramble_is_inverted() {
    let mut x = 0x1234_5678_9ABC_DEF1u64;
    for _ in 0..200_000 {
        let key = rng(&mut x);
        let n = rng(&mut x) & M40;
        let v = scramble(n, key);
        assert!(v <= M40);
        assert_eq!(seq_of(v, key), n);
        // any key gives a one-to-one map
        let other = rng(&mut x);
        assert_eq!(scramble(seq_of(v, other), other), v);
    }
    // the generator's own function, on the numbers from its source
    assert_eq!(SCR_C1.wrapping_mul(SCR_C1I) & M40, 1);
    assert_eq!(SCR_C2.wrapping_mul(SCR_C2I) & M40, 1);
}

#[test]
fn sequence_key_is_found() {
    let mut x = 0xDEAD_BEEF_1234_5678u64;
    for round in 0..6 {
        let key = rng(&mut x) & M40;
        let bound = [1u64 << 12, 50_000, 1 << 20][round % 3];
        let sample: Vec<u64> = (0..48).map(|_| scramble(rng(&mut x) % bound, key)).collect();
        let found = find_seq_key(&sample, bound, 3).expect("a key");
        assert!(sample.iter().all(|&v| seq_of(v, found) < bound));
    }
    // a few ids that are not generated ones (also in the first places) do not hide the key
    for round in 0..6 {
        let key = rng(&mut x) & M40;
        let bound = 1u64 << 20;
        let mut sample: Vec<u64> = (0..48).map(|_| scramble(rng(&mut x) % bound, key)).collect();
        for &at in [[0usize, 1, 17], [1, 2, 3], [0, 2, 47], [5, 6, 7], [0, 1, 2], [3, 30, 40]][round].iter() {
            sample[at] = rng(&mut x) & M40;
        }
        let found = find_seq_key(&sample, bound, 3).expect("a key despite three odd ids");
        assert_eq!(found, key);
    }
    // unrelated numbers have no key
    let sample: Vec<u64> = (0..48).map(|_| rng(&mut x) & M40).collect();
    assert_eq!(find_seq_key(&sample, 1 << 16, 3), None);
}

#[test]
fn user_codes_are_exact() {
    let mut x = 0x0BAD_C0DE_F00D_1234u64;
    let alphabet: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzuuAUZ_-`{:/ \xff\x00";
    let mut seen = std::collections::HashMap::new();
    let mut accepted = 0;
    for i in 0..300_000 {
        let n = rng(&mut x) % 12;
        let mut id: Vec<u8> = (0..n).map(|_| alphabet[(rng(&mut x) % alphabet.len() as u64) as usize]).collect();
        if i % 2 == 0 && !id.is_empty() {
            id[0] = b'u';
        }
        let b = padded(&id);
        let ok = id.len() >= 2 && id.len() <= 9 && id[0] == b'u' && id[1..].iter().all(|c| c.is_ascii_digit() || c.is_ascii_lowercase());
        let got = user_code(&b[..id.len()]);
        assert_eq!(got.is_some(), ok, "{:?}", String::from_utf8_lossy(&id));
        if let Some(code) = got {
            accepted += 1;
            assert!(code < 1 << 52);
            assert_eq!(user_code_key(code), key_of(&b[..id.len()]), "{:?}", String::from_utf8_lossy(&id));
            // different IDs, different codes
            if let Some(old) = seen.insert(code, id.clone()) {
                assert_eq!(old, id);
            }
        }
    }
    assert!(accepted > 10_000);
}

/// The remembered layouts give what the scanner gives, line for line.
#[test]
fn memo_scanner_matches() {
    fn fields_of<'a, const N: usize, const K: usize>(buf: &'a [u8], at: usize) -> (u8, usize, Vec<&'a [u8]>) {
        let p = unsafe { buf.as_ptr().add(at) };
        match unsafe { line_cursors::<N, K>(p) } {
            Line::Slow => (0, 0, Vec::new()),
            Line::Bad(e) => (1, e, Vec::new()),
            Line::Good(c) => {
                let mut f = unsafe { MaskFields::new(p, &c) };
                let mut v = Vec::new();
                for _ in 0..K {
                    v.push(f.lo());
                }
                for _ in K..N {
                    v.push(f.hi());
                }
                (2, c.e, v)
            }
        }
    }
    fn memo_of<'a, const N: usize, const K: usize>(buf: &'a [u8], at: usize, memo: &mut ShapeMemo) -> (u8, usize, Vec<&'a [u8]>) {
        let p = unsafe { buf.as_ptr().add(at) };
        match unsafe { line_memo::<N, K>(p, memo) } {
            LineM::Slow => (0, 0, Vec::new()),
            LineM::Bad(e) => (1, e, Vec::new()),
            LineM::Good(e, r, lo, hi) => {
                let mut f = MemoFields { p, pr: unsafe { p.add(r) }, lo, hi, i: 0, j: 0, _buf: std::marker::PhantomData };
                let mut v: Vec<&'a [u8]> = Vec::new();
                for _ in 0..K {
                    v.push(f.lo());
                }
                for _ in K..N {
                    v.push(f.hi());
                }
                (2, e, v)
            }
        }
    }
    let mut x = 0x0DDB_A11C_0FFE_E123u64;
    let mut memo7 = ShapeMemo::new();
    let mut memo4 = ShapeMemo::new();
    let mut good = [0usize; 2];
    for round in 0..300_000 {
        // a line of fields of a few lengths, most well formed, some with stray quotes, extra or missing separators
        let nf = match rng(&mut x) % 20 {
            0 => 3,
            1 => 6,
            2 => 8,
            3 => 5,
            _ => if round % 2 == 0 { 7 } else { 4 },
        };
        let mut line: Vec<u8> = Vec::new();
        for i in 0..nf {
            if i > 0 {
                line.push(b',');
            }
            let len = [0usize, 1, 4, 6, 8, 9, 13, 20, 25, 31][(rng(&mut x) % 10) as usize];
            let quoted = rng(&mut x) % 5 == 0;
            if quoted {
                line.push(b'"');
            }
            for _ in 0..len {
                line.push(match rng(&mut x) % 400 {
                    0 => b'"',
                    1 => b',',
                    _ => b'a' + (rng(&mut x) % 26) as u8,
                });
            }
            if quoted && rng(&mut x) % 50 != 0 {
                line.push(b'"');
            }
        }
        line.push(b'\n');
        // followed by another line and padding, as in a file
        line.extend_from_slice(b"zz,\"q\",zz,zz,zz,zz,zz\n");
        line.extend_from_slice(&[0u8; 160]);
        let a = fields_of::<7, 3>(&line, 0);
        let b = memo_of::<7, 3>(&line, 0, &mut memo7);
        assert_eq!(a, b, "7 fields: {:?}", String::from_utf8_lossy(&line[..line.len() - 160]));
        good[0] += (a.0 == 2) as usize;
        let a = fields_of::<4, 2>(&line, 0);
        let b = memo_of::<4, 2>(&line, 0, &mut memo4);
        assert_eq!(a, b, "4 fields: {:?}", String::from_utf8_lossy(&line[..line.len() - 160]));
        good[1] += (a.0 == 2) as usize;
    }
    assert!(good[0] > 20_000 && good[1] > 20_000, "{good:?}");
}

/// Keys of long strings kept by reference carry their first seven bytes:
/// every comparison must still be the byte-wise order of the strings.
#[test]
fn reference_keys_order_like_bytes() {
    let mut x = 0x1234_5678_9ABC_DEF1u64;
    // a small alphabet (with the zero byte) and shared prefixes, so that
    // prefixes of each other and equal first bytes are common
    let alphabet = [0u8, b'0', b'a', b'k', 0xFF];
    let stems: Vec<Vec<u8>> = (0..6)
        .map(|_| {
            let n = (rng(&mut x) % 12) as usize;
            (0..n).map(|_| alphabet[(rng(&mut x) % 5) as usize]).collect()
        })
        .collect();
    let mut texts: Vec<&'static [u8]> = Vec::new();
    for _ in 0..600 {
        let mut s = if rng(&mut x) % 3 == 0 { Vec::new() } else { stems[(rng(&mut x) % 6) as usize].clone() };
        let more = (rng(&mut x) % 30) as usize;
        for _ in 0..more {
            s.push(alphabet[(rng(&mut x) % 5) as usize]);
        }
        // (keys read 16 bytes from the start of a string)
        let text: &'static [u8] = Box::leak(padded(&s).into_boxed_slice());
        texts.push(&text[..s.len()]);
    }
    let keys: Vec<Key> = texts
        .iter()
        .map(|s| {
            if s.len() > 15 {
                assert!(ref_ok(s));
                let k = key_ref(s);
                assert_eq!(long_slice(&k), Some(*s));
                k
            } else {
                key_of(s)
            }
        })
        .collect();
    let mut by_ref = 0;
    for i in 0..texts.len() {
        for j in 0..texts.len() {
            by_ref += (is_ref(&keys[i]) || is_ref(&keys[j])) as usize;
            assert_eq!(cmp_key(&keys[i], &keys[j]), texts[i].cmp(texts[j]), "{:?} against {:?}", texts[i], texts[j]);
        }
    }
    assert!(by_ref > 10_000);
}
