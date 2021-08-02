use std::ops::{RangeBounds, Range};

use roaring::RoaringBitmap;

use super::ColTy;

pub struct TsCol {
    rows: Vec<u64>,
}

impl TsCol {
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn with_range(range: Range<u64>) -> Self {
        Self {
            rows: range.into_iter().collect(),
        }
    }

    pub fn put(&mut self, ts: u64) {
        self.rows.push(ts);
    }

    pub fn filter_with(&self, range: Range<u64>) -> RoaringBitmap {
        self.rows
            .iter()
            .enumerate()
            .filter(|(_idx, ts)| range.contains(ts))
            .map(|(idx, _)| idx as u32)
            .collect()
    }
}

impl ColTy for TsCol {}
