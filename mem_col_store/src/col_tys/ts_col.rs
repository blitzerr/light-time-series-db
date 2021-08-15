use std::ops::Range;

use roaring::RoaringBitmap;

use super::ColTy;

#[derive(Debug)]
pub struct TsCol {
    pub rows: Vec<u64>,
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

    pub fn with_values(values: Vec<u64>) -> Self {
        Self { rows: values }
    }

    pub fn put(&mut self, ts: u64) {
        self.rows.push(ts);
    }

    pub fn append(&mut self, vals: &mut Vec<u64>) {
        self.rows.append(vals);
    }

    /// scans all the time values to return the indices which contains a value that lies within the
    /// provided range. The set of indices is returned as a bitmap.
    pub fn filter_with(&self, range: Range<u64>) -> RoaringBitmap {
        self.rows
            .iter()
            .enumerate()
            .filter(|(_idx, ts)| range.contains(ts))
            .map(|(idx, _)| idx as u32)
            .collect()
    }

    pub fn get(&self, bm: &RoaringBitmap) -> Vec<u64> {
        // Clone below is okay as it only copies the heap pointer of the interned string.
        bm.into_iter()
            .map(|i| self.rows[i as usize].clone())
            .collect()
    }
}

impl ColTy for TsCol {}

#[cfg(test)]
mod tests {
    #[test]
    fn test_filter_with() {
        let ts_col = super::TsCol::with_range(0..100);
        let actual = ts_col.filter_with(47..59).iter().collect::<Vec<_>>();
        let expected = (47..59).collect::<Vec<_>>();
        assert_eq!(actual, expected);

        let ts_col = super::TsCol::with_range(100..200);
        let actual = ts_col.filter_with(147..159).iter().collect::<Vec<_>>();
        let expected = (47..59).collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }
}
