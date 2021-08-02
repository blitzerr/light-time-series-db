use core::f64;
use std::{
    collections::HashMap,
    ops::{Index, Range},
};

use internment::ArcIntern;

use crate::{Point, TagTy};

pub type TagStrTy = ArcIntern<String>;

#[derive(Debug)]
pub enum ColTy {
    /// A string column where instead of storing raw values, we intern them.
    Str(ArcIntern<String>),
    /// Should be used for the timestamp col.
    Time(u64),
    /// Floating values.
    F64(f64),
    /// A tag column can be key value where both of them are strings. Again to be space efficient, we
    /// intern the strings instead of storing raw ones.
    Tag(Vec<TagTy>),
}

#[derive(Debug)]
pub struct TimeseriesStore {
    source: Vec<ColTy>,
    metric: Vec<ColTy>,
    time: Vec<ColTy>,
    value: Vec<ColTy>,
    tags: Vec<ColTy>,

    metrics_map: HashMap<ArcIntern<String>, Vec<usize>>,
    // ts_map: BTreeMap<ArcIntern<String>, Vec<usize>>,
}

impl TimeseriesStore {
    pub fn new() -> Self {
        TimeseriesStore {
            source: Vec::with_capacity(100),
            metric: Vec::with_capacity(100),
            time: Vec::with_capacity(100),
            value: Vec::with_capacity(100),
            tags: Vec::with_capacity(100),

            metrics_map: HashMap::with_capacity(100),
            // ts_map: BTreeMap::new(),
        }
    }

    pub fn put_stream(&mut self, batch: Vec<Point>) -> color_eyre::Result<()> {
        batch.into_iter().for_each(|pt| {
            pt.vals.iter().for_each(|val| {
                let source = ArcIntern::new(pt.source.clone());
                let metric = ArcIntern::new(pt.metric.clone());

                self.source.push(ColTy::Str(source));

                self.metrics_map
                    .entry(metric.clone())
                    .or_default()
                    .push(self.metric.len());
                self.metric.push(ColTy::Str(metric));

                // self.ts_map
                //     .entry(val.timestamp)
                //     .or_default()
                //     .push(self.time.len());
                self.time.push(ColTy::Time(val.timestamp));

                self.value.push(ColTy::F64(val.value));
                self.tags.push(ColTy::Tag(val.tags.clone()));
            });
        });
        Ok(())
    }

    pub fn query(
        &self,
        time_range: Range<u64>,
        metrics: &Vec<&str>,
        _tags: &Vec<(String, String)>,
    ) {
        let metric_arc_interns = metrics
            .iter()
            .map(|&m| self.metrics_map.get(&ArcIntern::new(m.to_owned())))
            .flatten()
            .flatten()
            .collect::<Vec<&usize>>();

        // apply timerange filter
        let result: Vec<(&ColTy, &ColTy, &ColTy, &ColTy, &ColTy)> = metric_arc_interns
            .iter()
            .filter_map(|&i| match self.time.index(*i) {
                ColTy::Str(_) => None,
                ColTy::Time(t) => {
                    if time_range.contains(t) {
                        Some((
                            &self.source[*i],
                            &self.metric[*i],
                            &self.time[*i],
                            &self.value[*i],
                            &self.tags[*i],
                        ))
                    } else {
                        None
                    }
                }
                ColTy::F64(_) => None,
                ColTy::Tag(_) => None,
            })
            .collect();
        println!("{:?}", result);
    }
}

#[cfg(test)]
mod tests {
    use internment::ArcIntern;

    use crate::{col_store::TimeseriesStore, search_based};

    #[test]
    fn t_intern() {
        let hello = ArcIntern::new("hello");
        let hello2 = ArcIntern::new("hello");
        let world = ArcIntern::new("world");

        println!("{}, {}", hello, hello2);
        assert_eq!(hello, hello2);
        assert_ne!(hello, world);
        assert_eq!(ArcIntern::<&str>::num_objects_interned(), 2);
        assert_eq!(hello.refcount(), 2);
        assert_eq!(world.refcount(), 1);
    }

    #[test]
    fn t_query() {
        let points = crate::search_based::tests::gen_points(
            &vec!["server", "database"],
            &vec!["cpu", "mem"],
            1627753205000,
            1627753205007,
        );
        let mut t_store = TimeseriesStore::new();
        t_store.put_stream(points).unwrap();
        t_store.query(1627753205000..1627753205002, &vec!["cpu"], &vec![]);
    }
}
