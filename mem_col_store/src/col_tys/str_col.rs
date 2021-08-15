use internment::ArcIntern;
use roaring::RoaringBitmap;

use super::{ColTy, Str};

#[derive(Debug)]
pub struct StrCol {
    pub rows: Vec<Str>,
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

    pub fn append(&mut self, vals: &Vec<&str>) {
        let mut r: Vec<_> = vals.iter().map(|&x| ArcIntern::new(x.to_owned())).collect();
        self.rows.append(&mut r);
    }

    pub fn filter_with(&self, indices_to_consider: RoaringBitmap, key: &str) -> RoaringBitmap {
        let key = ArcIntern::new(key.to_owned());
        indices_to_consider
            .into_iter()
            .filter(|idx| key == self.rows[*idx as usize])
            .collect()
    }

    pub fn get(&self, bm: &RoaringBitmap) -> Vec<Str> {
        // Clone below is okay as it only copies the heap pointer of the interned string.
        bm.into_iter()
            .map(|i| self.rows[i as usize].clone())
            .collect()
    }
}

impl ColTy for StrCol {}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;

    use super::StrCol;

    #[test]
    fn test_filter_with() {
        let s_col_strs = vec![
            "hello", "world", "how", "are", "you", "all", "doing", "!", "hiya",
        ];
        let mut s_col =
                                    // 0           1       2       3     4       5      6
            StrCol::with_strs(vec!["hello", "world", "how", "are", "you", "world", "doing", "world"]);
        // 7
        s_col.append(&vec!["!"]);
        // 8
        s_col.append(&vec!["hiya", "once", "more"]);

        let mut region_of_interest = RoaringBitmap::new();
        region_of_interest.insert_range(1..6);

        assert_eq!(
            vec![1, 5],
            s_col
                .filter_with(region_of_interest, "world")
                .iter()
                .collect::<Vec<_>>()
        );
    }
}
