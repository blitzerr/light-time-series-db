use std::collections::{HashMap, HashSet};

use roaring::RoaringBitmap;
use serde_json::json;

use super::{ColTy, Str};

/// Each row of this column can be tagged with multiple tags.
pub type Tags = HashMap<Str, HashSet<Str>>;

pub fn tags_from_str(s: &str) -> color_eyre::Result<Tags> {
    let tags = serde_json::from_str(s)?;
    Ok(tags)
}

#[derive(Debug)]
pub struct TagCol {
    /// All rows in the column.
    pub rows: Vec<Tags>,
}

impl ColTy for TagCol {}
impl TagCol {
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Let's you tag one row at a time.
    pub fn append(&mut self, tags: Tags) {
        self.rows.push(tags);
    }

    /// If any of the the tags of the row matches any of the tags we are looking for, return the row
    /// indices as a bitmap.
    pub fn filter_with(&self, indices_to_consider: RoaringBitmap, keys: &Tags) -> RoaringBitmap {
        // let tags = to_tags(keys);
        // // Convert the vec of vec of keys to Map of set, that way look up will be constant time.
        // let key_map: HashMap<_, HashSet<_>> = tags
        //     .into_iter()
        //     .map(|(k, v)| (k, v.into_iter().collect()))
        //     .collect();

        // Now we traverse the data one at a time. An element of the vec of data is of the form,
        // (key, [values]). So, first we try to check if the key is present in the key_map
        // constructed above. If the key is present, then we try to check if any of the values is
        // present in the set indexed with the key in the key_map. If both of these are true, we
        // return the index of the data element as a bitset.
        indices_to_consider
            .into_iter()
            .filter(|idx| {
                self.rows[*idx as usize].iter().any(|(k, vals)| {
                    keys.get(k)
                        .map(|tag_vals| tag_vals.is_disjoint(vals) == false)
                        .unwrap_or_default()
                })
            })
            .collect()
    }
}

pub fn get_dummy_tag_col() -> TagCol {
    let tag1 = tags_from_str(
        &json!({
            "shard" : ["0", "59"],
            "version": ["9.3"],
            "app": ["postgres"],
            "server": ["192.168.0.1"]
        })
        .to_string(),
    )
    .unwrap();

    let tag2 = tags_from_str(
        &json!({
            "shard" : ["23", "36"],
            "version": ["1.0"],
            "app": ["tomcat"],
            "server": ["192.168.0.1"]
        })
        .to_string(),
    )
    .unwrap();

    let tag3 = tags_from_str(
        &json!({
            "shard" : ["56", "6"],
            "version": ["5.0"],
            "app": ["zookeeper"]
        })
        .to_string(),
    )
    .unwrap();

    let tag4 = tags_from_str(
        &json!({
            "shard" : ["0", "59"],
            "version": ["8.4"],
            "app": ["postgres"]
        })
        .to_string(),
    )
    .unwrap();

    let mut tag_col = TagCol::new();
    tag_col.append(tag1);
    tag_col.append(tag2);
    tag_col.append(tag3);
    tag_col.append(tag4);

    tag_col
}

#[cfg(test)]
pub mod tests {
    use std::collections::{HashMap, HashSet};

    use roaring::RoaringBitmap;
    use serde_json::json;

    use crate::col_tys::tag_col::tags_from_str;

    use super::{get_dummy_tag_col, TagCol};

    #[test]
    fn t_tag_filter() {
        let tag_col = get_dummy_tag_col();
        let mut idxs_to_consider = RoaringBitmap::new();
        idxs_to_consider.insert_range(0..3);
        let keys = tags_from_str(
            &json!({
                "not_present": ["anything", "doesn't matter"],
                "app": ["postgres", "tomcat"]
            })
            .to_string(),
        )
        .unwrap();

        let m = tag_col.filter_with(idxs_to_consider, &keys);
        let mut mm = RoaringBitmap::new();
        mm.insert(0);
        mm.insert(1);

        assert_eq!(mm, m);
    }
}
