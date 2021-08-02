use std::collections::{HashMap, HashSet};

use internment::ArcIntern;
use roaring::RoaringBitmap;

use super::{ColTy, Str};

/// A tag is a key/value pair where the value can be a list.
pub type TagsKeyVal = (Str, Vec<Str>);

/// Each row of this column can be tagged with multiple tags.
pub type Tags = Vec<TagsKeyVal>;
pub struct TagCol {
    /// All rows in the column.
    rows: Vec<Tags>,
}

impl ColTy for TagCol {}
impl TagCol {
    pub fn new() -> Self {
        Self { rows: Vec::new() }
    }

    /// Let's you tag one row at a time.
    pub fn put_tags(&mut self, tags: Vec<(&str, Vec<&str>)>) {
        let tags = to_tags(tags);
        self.rows.push(tags);
    }

    /// If any of the the tags of the row matches any of the tags we are looking for, return the row
    /// indices as a bitmap.
    pub fn filter_with(&self, keys: Vec<(&str, Vec<&str>)>) -> RoaringBitmap {
        let tags = to_tags(keys);
        // Convery the vec of vec of keys to Map of set, that way look up will be constant time.
        let key_map: HashMap<_, HashSet<_>> = tags
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect();

        // Now we traverse the data one at a time. An element of the vec of data is of the form,
        // (key, [values]). So, first we try to check if the key is present in the key_map
        // constructed above. If the key is present, then we try to check if any of the values is
        // present in the set indexed with the key in the key_map. If both of these are true, we
        // return the index of the data element as a bitset.
        self.rows
            .iter()
            .enumerate()
            .filter(|(_row_num, tags)| {
                tags.iter().any(|(k, vals)| {
                    key_map
                        .get(k)
                        .and_then(|set| {
                            let any_match = vals.iter().any(|val| {
                                set.contains(val)
                            });
                            if any_match {
                                Some(true)
                            } else {
                                None
                            }
                        })
                        .is_some()
                })
            })
            .map(|(row_num, _)| row_num as u32)
            .collect()
    }
}

fn to_tags(tags: Vec<(&str, Vec<&str>)>) -> Tags {
    tags.iter()
        .map(|(key, val_list)| {
            let interned_key = ArcIntern::new(key.to_owned().to_owned());
            let val_list_interned: Vec<Str> = val_list
                .iter()
                .map(|&v| ArcIntern::new(v.to_owned()))
                .collect();

            (interned_key, val_list_interned)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use roaring::RoaringBitmap;

    use super::TagCol;

    #[test]
    fn t_predicate() {
        let data = vec![
            ("k0", vec!["kshkhj", "world"]),
            ("k1", vec!["abc", "def"]),
            ("k2", vec!["xyz"]),
        ];

        let keys = vec![("k0", vec!["world", "happy"]), ("k1", vec!["abc", "pqr"])];

        let m: HashMap<_, HashSet<_>> = keys
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect();

        let xx: Vec<_> = data
            .iter()
            .enumerate()
            .filter(|(i, (k, v))| {
                m.get(k.to_owned())
                    .and_then(|set| Some(v.iter().any(|data_val| set.contains(data_val))))
                    .is_some()
            })
            .map(|(i, _)| i)
            .collect();
        assert_eq!(vec![0, 1], xx);
    }

    #[test]
    fn t_tag_filter() {
        let mut tag_col = TagCol::new();
        tag_col.put_tags(vec![
            ("shard", vec!["0", "59"]),
            ("version", vec!["9.3"]),
            ("app", vec!["postgres"]),
            ("server", vec!["192.168.0.1"]),
        ]);
        tag_col.put_tags(vec![
            ("shard", vec!["23", "36"]),
            ("version", vec!["1.0"]),
            ("app", vec!["tomcat"]),
            ("server", vec!["192.168.0.1"]),
        ]);
        tag_col.put_tags(vec![
            ("shard", vec!["56", "6"]),
            ("version", vec!["5.0"]),
            ("app", vec!["zookeeper"]),
        ]);
        tag_col.put_tags(vec![
            ("shard", vec!["0", "59"]),
            ("version", vec!["8.4"]),
            ("app", vec!["postgres"]),
        ]);

        let m = tag_col.filter_with(vec![("app", vec!["postgres", "tomcat"])]);
        let mut mm = RoaringBitmap::new();
        mm.insert(0);
        mm.insert(1);
        mm.insert(3);

        assert_eq!(mm, m);
    }
}
