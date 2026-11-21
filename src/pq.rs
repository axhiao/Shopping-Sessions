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
    pub fn release(&self) {
        self.values.release();
        self.defs.release();
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
