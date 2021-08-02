use std::slice::Iter;

use internment::ArcIntern;
use roaring::RoaringBitmap;

use super::{ColTy, Str};

pub struct StrCol {
    rows: Vec<Str>,
}

impl StrCol {
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn with_strs(rows: Vec<&str>) -> Self {
        Self {
            rows: rows.iter().map(|&x| ArcIntern::new(x.to_owned())).collect(),
        }
    }

    pub fn filter_with(&self, keys: &Vec<&str>) -> RoaringBitmap {
        let matchers: Vec<ArcIntern<String>> =
            keys.iter().map(|&x| ArcIntern::new(x.to_owned())).collect();
        self.rows
            .iter()
            .enumerate()
            .filter(|(_idx, x)| matchers.contains(x))
            .map(|(idx, _)| idx as u32)
            .collect()
    }
}

impl ColTy for StrCol {}
