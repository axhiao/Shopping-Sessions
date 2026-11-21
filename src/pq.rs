//! Minimal Parquet writer: one row group, one PLAIN data page per column,
//! uncompressed. Enough for the two output tables (INT32 DATE, INT32/INT64,
//! UTF8 strings, one nullable string column). Buffers live in the arena so
//! writing never calls mmap/munmap (which would contend with page faults).

use crate::PVec;
use std::io::Write;

#[derive(Clone, Copy, PartialEq)]
pub enum Kind {
    Str,
    Date,
    I32,
    I64,
}

pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
    pub optional: bool,
    /// PLAIN-encoded non-null values.
    values: PVec<u8>,
    /// Definition levels, one bit per row, LSB first (only when optional).
    defs: PVec<u8>,
    rows: usize,
}

impl Column {
    pub fn new(name: &'static str, kind: Kind, optional: bool, rows: usize) -> Column {
        let per = match kind {
            Kind::Str => 20,
            Kind::Date | Kind::I32 => 4,
            Kind::I64 => 8,
        };
        Column {
            name,
            kind,
            optional,
            values: PVec::with_capacity(rows * per + 64),
            defs: PVec::with_capacity(if optional { rows / 8 + 16 } else { 16 }),
            rows: 0,
        }
    }
    #[inline]
    fn def(&mut self, present: bool) {
        if self.optional {
            if self.rows % 8 == 0 {
                self.defs.push(0);
            }
            if present {
                let last = self.defs.len() - 1;
                unsafe { *self.defs.ptr.add(last) |= 1 << (self.rows % 8) };
            }
        }
        self.rows += 1;
    }
    #[inline]
    pub fn push_str(&mut self, s: &[u8]) {
        self.values.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.values.extend_from_slice(s);
        self.def(true);
    }
    #[inline]
    pub fn push_null(&mut self) {
        self.def(false);
    }
    #[inline]
    pub fn push_i32(&mut self, v: i32) {
        self.values.extend_from_slice(&v.to_le_bytes());
        self.rows += 1;
    }
    #[inline]
    pub fn push_i64(&mut self, v: i64) {
        self.values.extend_from_slice(&v.to_le_bytes());
        self.rows += 1;
    }
}

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
    cur: i16,
}

impl Thrift {
    fn new() -> Thrift {
        Thrift { buf: Vec::new(), last: Vec::new(), cur: 0 }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let d = id - self.cur;
        if d > 0 && d <= 15 {
            self.buf.push(((d as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
        self.cur = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zigzag(v);
    }
    fn bin(&mut self, id: i16, b: &[u8]) {
        self.field(id, 8);
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn begin_struct(&mut self, id: i16) {
        self.field(id, 12);
        self.push_scope();
    }
    fn push_scope(&mut self) {
        self.last.push(self.cur);
        self.cur = 0;
    }
    fn end_struct(&mut self) {
        self.buf.push(0);
        self.cur = self.last.pop().unwrap();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
    fn list_i32(&mut self, v: i32) {
        self.zigzag(v as i64);
    }
    fn list_bin(&mut self, b: &[u8]) {
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
}

fn ptype(k: Kind) -> i32 {
    match k {
        Kind::Str => 6,              // BYTE_ARRAY
        Kind::Date | Kind::I32 => 1, // INT32
        Kind::I64 => 2,              // INT64
    }
}

/// RLE/bit-packed hybrid encoding of 1-bit definition levels: bit-packed runs
/// of up to 63 groups of 8, straight from the packed bits.
fn encode_defs(bits: &[u8], out: &mut Vec<u8>) {
    let mut body = Vec::with_capacity(bits.len() + bits.len() / 63 + 8);
    for run in bits.chunks(63) {
        body.push(((run.len() << 1) | 1) as u8);
        body.extend_from_slice(run);
    }
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
}

pub fn write_file(path: &std::path::Path, cols: &[Column], nrows: usize) -> std::io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    f.write_all(b"PAR1")?;
    let mut pos: i64 = 4;
    // (data_page_offset, total_size)
    let mut metas: Vec<(i64, i64)> = Vec::new();
    if nrows > 0 {
        for c in cols {
            assert_eq!(c.rows, nrows);
            let mut pre = Vec::new();
            if c.optional {
                encode_defs(c.defs.as_slice(), &mut pre);
            }
            let body_len = pre.len() + c.values.len();
            let mut h = Thrift::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, body_len as i32);
            h.i32(3, body_len as i32);
            h.begin_struct(5);
            h.i32(1, nrows as i32);
            h.i32(2, 0); // PLAIN
            h.i32(3, 3); // RLE
            h.i32(4, 3); // RLE
            h.end_struct();
            h.buf.push(0);
            f.write_all(&h.buf)?;
            f.write_all(&pre)?;
            f.write_all(c.values.as_slice())?;
            let size = (h.buf.len() + body_len) as i64;
            metas.push((pos, size));
            pos += size;
        }
    }
    let mut t = Thrift::new();
    t.i32(1, 1);
    t.list(2, 12, cols.len() + 1);
    // root
    t.push_scope();
    t.bin(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end_struct();
    for c in cols {
        t.push_scope();
        t.i32(1, ptype(c.kind));
        t.i32(3, if c.optional { 1 } else { 0 });
        t.bin(4, c.name.as_bytes());
        match c.kind {
            Kind::Str => t.i32(6, 0),  // UTF8
            Kind::Date => t.i32(6, 6), // DATE
            _ => {}
        }
        match c.kind {
            Kind::Str => {
                t.begin_struct(10);
                t.begin_struct(1);
                t.end_struct();
                t.end_struct();
            }
            Kind::Date => {
                t.begin_struct(10);
                t.begin_struct(6);
                t.end_struct();
                t.end_struct();
            }
            _ => {}
        }
        t.end_struct();
    }
    t.i64(3, nrows as i64);
    if nrows > 0 {
        t.list(4, 12, 1);
        t.push_scope();
        t.list(1, 12, cols.len());
        let mut total = 0i64;
        for (c, &(off, size)) in cols.iter().zip(&metas) {
            total += size;
            t.push_scope();
            t.i64(2, off);
            t.begin_struct(3);
            t.i32(1, ptype(c.kind));
            t.list(2, 5, 2);
            t.list_i32(0);
            t.list_i32(3);
            t.list(3, 8, 1);
            t.list_bin(c.name.as_bytes());
            t.i32(4, 0);
            t.i64(5, nrows as i64);
            t.i64(6, size);
            t.i64(7, size);
            t.i64(9, off);
            t.end_struct();
            t.end_struct();
        }
        t.i64(2, total);
        t.i64(3, nrows as i64);
        t.end_struct();
    } else {
        t.list(4, 12, 0);
    }
    t.bin(6, b"shopping-sessions");
    t.buf.push(0);
    let n = t.buf.len() as u32;
    t.buf.extend_from_slice(&n.to_le_bytes());
    t.buf.extend_from_slice(b"PAR1");
    f.write_all(&t.buf)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// user_daily writer: rows go straight into small reusable column buffers and
// are written as a file every few tens of thousands of persons, with
// dictionary encodings:
//   user_id       dictionary of the file's persons, one RLE run per person
//   day           dictionary of the window's days, bit-packed indices
//   sessions      identity dictionary + bit-packed when small, else PLAIN
//   events        same
//   revenue_cents PLAIN INT64
//   top_category  dictionary of all categories, bit-packed indices

const PERSONS_PER_FILE: usize = 32 << 10;
/// Values below this may use an identity dictionary.
const IDENTITY_MAX: u32 = 1 << 12;

pub struct UserWriter {
    dir: std::path::PathBuf,
    worker: usize,
    seq: usize,
    day0: i32,
    ndays: usize,
    /// PLAIN dictionary page of the categories, and their count.
    cat_dict: Vec<u8>,
    ncat: usize,
    // per row
    days: Vec<u8>,
    sessions: Vec<u32>,
    events: Vec<u32>,
    revenue: Vec<u64>,
    top: Vec<u16>, // category index, 0xFFFF = null
    // per person
    runs: Vec<u32>,
    names: Vec<u8>, // PLAIN byte arrays
    person_rows: u32,
    out: Vec<u8>,
    tmp: Vec<u8>,
    pub rows: usize,
    pub files: usize,
}

struct Chunk {
    dict_off: i64, // 0 = no dictionary page
    data_off: i64,
    size: i64,
    encoding: i32,
}

fn bits_for(max: u32) -> u32 {
    (32 - max.leading_zeros()).max(1)
}

/// RLE/bit-packed hybrid: `vals` as bit-packed runs of up to 63 groups of 8.
fn bitpack<I: Iterator<Item = u32>>(vals: I, bw: u32, out: &mut Vec<u8>) {
    let mut vals = vals.peekable();
    while vals.peek().is_some() {
        let head = out.len();
        out.push(0);
        let mut groups = 0usize;
        let (mut acc, mut nbits) = (0u64, 0u32);
        while groups < 63 && vals.peek().is_some() {
            for _ in 0..8 {
                let v = vals.next().unwrap_or(0) as u64;
                acc |= v << nbits;
                nbits += bw;
                while nbits >= 8 {
                    out.push(acc as u8);
                    acc >>= 8;
                    nbits -= 8;
                }
            }
            // 8 values of bw bits end on a byte boundary
            groups += 1;
        }
        out[head] = ((groups << 1) | 1) as u8;
    }
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn dict_page_header(out: &mut Vec<u8>, body: usize, nvalues: usize) {
    let mut h = Thrift::new();
    h.i32(1, 2); // DICTIONARY_PAGE
    h.i32(2, body as i32);
    h.i32(3, body as i32);
    h.begin_struct(7);
    h.i32(1, nvalues as i32);
    h.i32(2, 0); // PLAIN
    h.end_struct();
    h.buf.push(0);
    out.extend_from_slice(&h.buf);
}

fn data_page_header(out: &mut Vec<u8>, body: usize, nrows: usize, encoding: i32) {
    let mut h = Thrift::new();
    h.i32(1, 0); // DATA_PAGE
    h.i32(2, body as i32);
    h.i32(3, body as i32);
    h.begin_struct(5);
    h.i32(1, nrows as i32);
    h.i32(2, encoding);
    h.i32(3, 3); // RLE
    h.i32(4, 3); // RLE
    h.end_struct();
    h.buf.push(0);
    out.extend_from_slice(&h.buf);
}

const PLAIN: i32 = 0;
const RLE_DICTIONARY: i32 = 8;

impl UserWriter {
    pub fn new(dir: &std::path::Path, worker: usize, day0: i32, ndays: usize, cat_names: &[Vec<u8>]) -> UserWriter {
        let mut cat_dict = Vec::new();
        for c in cat_names {
            cat_dict.extend_from_slice(&(c.len() as u32).to_le_bytes());
            cat_dict.extend_from_slice(c);
        }
        let rows = PERSONS_PER_FILE * 5;
        UserWriter {
            dir: dir.to_path_buf(),
            worker,
            seq: 0,
            day0,
            ndays,
            cat_dict,
            ncat: cat_names.len(),
            days: Vec::with_capacity(rows),
            sessions: Vec::with_capacity(rows),
            events: Vec::with_capacity(rows),
            revenue: Vec::with_capacity(rows),
            top: Vec::with_capacity(rows),
            runs: Vec::with_capacity(PERSONS_PER_FILE),
            names: Vec::with_capacity(PERSONS_PER_FILE * 16),
            person_rows: 0,
            out: Vec::new(),
            tmp: Vec::new(),
            rows: 0,
            files: 0,
        }
    }

    /// One row of the current person; `top` < 0 is null.
    #[inline(always)]
    pub fn row(&mut self, day_idx: usize, sessions: u32, events: u32, revenue: u64, top: i32) {
        self.days.push(day_idx as u8);
        self.sessions.push(sessions);
        self.events.push(events);
        self.revenue.push(revenue);
        self.top.push(top as u16);
        self.person_rows += 1;
    }

    /// Ends the current person (who has at least one row).
    #[inline(always)]
    pub fn end_person(&mut self, name: &[u8]) {
        self.names.extend_from_slice(&(name.len() as u32).to_le_bytes());
        self.names.extend_from_slice(name);
        self.runs.push(self.person_rows);
        self.person_rows = 0;
        if self.runs.len() >= PERSONS_PER_FILE {
            self.flush();
        }
    }

    pub fn finish(&mut self, always: bool) {
        if !self.runs.is_empty() || (always && self.files == 0) {
            self.flush();
        }
    }

    #[cold]
    #[inline(never)]
    fn flush(&mut self) {
        let nrows = self.days.len();
        let mut out = std::mem::take(&mut self.out);
        let mut tmp = std::mem::take(&mut self.tmp);
        out.clear();
        out.extend_from_slice(b"PAR1");
        let mut chunks: Vec<Chunk> = Vec::new();
        if nrows > 0 {
            // user_id
            {
                let dict_off = out.len() as i64;
                dict_page_header(&mut out, self.names.len(), self.runs.len());
                out.extend_from_slice(&self.names);
                let bw = bits_for(self.runs.len() as u32 - 1);
                let bytes = (bw as usize).div_ceil(8);
                tmp.clear();
                tmp.push(bw as u8);
                for (i, &n) in self.runs.iter().enumerate() {
                    varint(&mut tmp, (n as u64) << 1);
                    tmp.extend_from_slice(&(i as u32).to_le_bytes()[..bytes]);
                }
                let data_off = out.len() as i64;
                data_page_header(&mut out, tmp.len(), nrows, RLE_DICTIONARY);
                out.extend_from_slice(&tmp);
                chunks.push(Chunk { dict_off, data_off, size: out.len() as i64 - dict_off, encoding: RLE_DICTIONARY });
            }
            // day
            {
                let dict_off = out.len() as i64;
                dict_page_header(&mut out, self.ndays * 4, self.ndays);
                for d in 0..self.ndays {
                    out.extend_from_slice(&(self.day0 + d as i32).to_le_bytes());
                }
                let bw = bits_for(self.ndays as u32 - 1);
                tmp.clear();
                tmp.push(bw as u8);
                bitpack(self.days.iter().map(|&d| d as u32), bw, &mut tmp);
                let data_off = out.len() as i64;
                data_page_header(&mut out, tmp.len(), nrows, RLE_DICTIONARY);
                out.extend_from_slice(&tmp);
                chunks.push(Chunk { dict_off, data_off, size: out.len() as i64 - dict_off, encoding: RLE_DICTIONARY });
            }
            // sessions, events
            for vals in [&self.sessions, &self.events] {
                let max = vals.iter().copied().max().unwrap_or(0);
                if max < IDENTITY_MAX {
                    let dict_off = out.len() as i64;
                    dict_page_header(&mut out, (max as usize + 1) * 4, max as usize + 1);
                    for v in 0..=max {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                    let bw = bits_for(max);
                    tmp.clear();
                    tmp.push(bw as u8);
                    bitpack(vals.iter().copied(), bw, &mut tmp);
                    let data_off = out.len() as i64;
                    data_page_header(&mut out, tmp.len(), nrows, RLE_DICTIONARY);
                    out.extend_from_slice(&tmp);
                    chunks.push(Chunk { dict_off, data_off, size: out.len() as i64 - dict_off, encoding: RLE_DICTIONARY });
                } else {
                    let data_off = out.len() as i64;
                    data_page_header(&mut out, nrows * 4, nrows, PLAIN);
                    for v in vals.iter() {
                        out.extend_from_slice(&v.to_le_bytes());
                    }
                    chunks.push(Chunk { dict_off: 0, data_off, size: out.len() as i64 - data_off, encoding: PLAIN });
                }
            }
            // revenue_cents
            {
                let data_off = out.len() as i64;
                data_page_header(&mut out, nrows * 8, nrows, PLAIN);
                for v in self.revenue.iter() {
                    out.extend_from_slice(&v.to_le_bytes());
                }
                chunks.push(Chunk { dict_off: 0, data_off, size: out.len() as i64 - data_off, encoding: PLAIN });
            }
            // top_category
            {
                let dict_off = out.len() as i64;
                dict_page_header(&mut out, self.cat_dict.len(), self.ncat);
                out.extend_from_slice(&self.cat_dict);
                tmp.clear();
                tmp.extend_from_slice(&[0u8; 4]);
                bitpack(self.top.iter().map(|&t| (t != 0xFFFF) as u32), 1, &mut tmp);
                let dl = (tmp.len() - 4) as u32;
                tmp[..4].copy_from_slice(&dl.to_le_bytes());
                let bw = bits_for(self.ncat as u32 - 1);
                tmp.push(bw as u8);
                bitpack(self.top.iter().filter(|&&t| t != 0xFFFF).map(|&t| t as u32), bw, &mut tmp);
                let data_off = out.len() as i64;
                data_page_header(&mut out, tmp.len(), nrows, RLE_DICTIONARY);
                out.extend_from_slice(&tmp);
                chunks.push(Chunk { dict_off, data_off, size: out.len() as i64 - dict_off, encoding: RLE_DICTIONARY });
            }
        }
        // footer
        let cols: [(&str, Kind, bool); 6] = [
            ("user_id", Kind::Str, false),
            ("day", Kind::Date, false),
            ("sessions", Kind::I32, false),
            ("events", Kind::I32, false),
            ("revenue_cents", Kind::I64, false),
            ("top_category", Kind::Str, true),
        ];
        let mut t = Thrift::new();
        t.i32(1, 1);
        t.list(2, 12, cols.len() + 1);
        t.push_scope();
        t.bin(4, b"schema");
        t.i32(5, cols.len() as i32);
        t.end_struct();
        for &(name, kind, optional) in &cols {
            t.push_scope();
            t.i32(1, ptype(kind));
            t.i32(3, if optional { 1 } else { 0 });
            t.bin(4, name.as_bytes());
            match kind {
                Kind::Str => {
                    t.i32(6, 0);
                    t.begin_struct(10);
                    t.begin_struct(1);
                    t.end_struct();
                    t.end_struct();
                }
                Kind::Date => {
                    t.i32(6, 6);
                    t.begin_struct(10);
                    t.begin_struct(6);
                    t.end_struct();
                    t.end_struct();
                }
                _ => {}
            }
            t.end_struct();
        }
        t.i64(3, nrows as i64);
        if nrows > 0 {
            t.list(4, 12, 1);
            t.push_scope();
            t.list(1, 12, cols.len());
            let mut total = 0i64;
            for (&(name, kind, _), c) in cols.iter().zip(&chunks) {
                total += c.size;
                let first = if c.dict_off != 0 { c.dict_off } else { c.data_off };
                t.push_scope();
                t.i64(2, first);
                t.begin_struct(3);
                t.i32(1, ptype(kind));
                if c.encoding == PLAIN {
                    t.list(2, 5, 2);
                    t.list_i32(PLAIN);
                    t.list_i32(3);
                } else {
                    t.list(2, 5, 3);
                    t.list_i32(PLAIN);
                    t.list_i32(3);
                    t.list_i32(RLE_DICTIONARY);
                }
                t.list(3, 8, 1);
                t.list_bin(name.as_bytes());
                t.i32(4, 0);
                t.i64(5, nrows as i64);
                t.i64(6, c.size);
                t.i64(7, c.size);
                t.i64(9, c.data_off);
                if c.dict_off != 0 {
                    t.i64(11, c.dict_off);
                }
                t.end_struct();
                t.end_struct();
            }
            t.i64(2, total);
            t.i64(3, nrows as i64);
            t.end_struct();
        } else {
            t.list(4, 12, 0);
        }
        t.bin(6, b"shopping-sessions");
        t.buf.push(0);
        let n = t.buf.len() as u32;
        out.extend_from_slice(&t.buf);
        out.extend_from_slice(&n.to_le_bytes());
        out.extend_from_slice(b"PAR1");
        let path = self.dir.join(format!("part-{:03}-{:04}.parquet", self.worker, self.seq));
        std::fs::write(&path, &out).expect("write user_daily");
        self.seq += 1;
        self.files += 1;
        self.rows += nrows;
        self.days.clear();
        self.sessions.clear();
        self.events.clear();
        self.revenue.clear();
        self.top.clear();
        self.runs.clear();
        self.names.clear();
        self.out = out;
        self.tmp = tmp;
    }
}
