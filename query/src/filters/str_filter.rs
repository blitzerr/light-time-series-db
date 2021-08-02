use std::fmt;

use mem_col_store::col_tys::str_col::StrCol;

#[cfg(test)]
mod tests {
    use mem_col_store::col_tys::{str_col::StrCol, ts_col::TsCol};
    use roaring::RoaringBitmap;

    #[test]
    fn t_str_filter() {
        let str_matches = StrCol::with_strs(vec!["hello", "world", "how", "are", "you?"])
            .filter_with(&vec!["how", "hello"]);
        let mut bm = RoaringBitmap::new();
        bm.insert(0);
        bm.insert(2);
        assert_eq!(str_matches, bm);

        let tss = TsCol::with_range(0..4);
        let ts_matches = tss.filter_with(1..3);

        let mut tm = RoaringBitmap::new();
        tm.insert(1);
        tm.insert(2);
        assert_eq!(ts_matches, tm);

        let intersect = bm & tm;
        let mut im = RoaringBitmap::new();
        im.insert(2);
        assert_eq!(im, intersect);
    }
}
