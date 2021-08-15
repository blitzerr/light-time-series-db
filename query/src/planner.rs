use std::ops::Range;

use crate::{
    aggs::median_vec,
    qry::{Agg, Query},
};
use either::{Either::Right, Left};
use mem_col_store::col_tys::{
    str_col::StrCol,
    tag_col::{TagCol, Tags},
    ts_col::TsCol,
    val_col::ValCol,
};
use roaring::RoaringBitmap;

/// Given a timewindow as a start and an end time, this method chunks it up into smaller intervals
/// of `sz`. The chunks are returned as a vector of ranges.
fn chunk_time(start: u64, end: u64, sz: u64) -> Vec<Range<u64>> {
    let window_sz = end - start;
    if window_sz > sz {
        let sz = sz as usize;
        let iter1 = (start..=end).step_by(sz);
        let iter2 = (start..=end).step_by(sz).skip(1);

        // If the window is not perfectly divisible by the size, then there will be some left over
        // that we ,ust capture in the last iteration.
        let iter3 = if window_sz % sz as u64 != 0 {
            Left(iter2.chain(Some(end)))
        } else {
            Right(iter2)
        };
        iter1.zip(iter3).map(|(s, e)| s..e).collect::<Vec<_>>()
        // if window_sz % sz as u64 != 0 {
        //     let last_range = x.last().unwrap().end;
        //     x.push(last_range..end);
        // }
        // x
    } else {
        vec![start..end]
    }
}

/// This returns a lambda that will take the timestampo column and return the starting index of the
/// first row that falls within the timestamp.
fn filter_by_time(range: Range<u64>) -> impl FnOnce(&TsCol) -> RoaringBitmap {
    |ts_col: &TsCol| ts_col.filter_with(range)
}

fn filter_by_metrics(metric: &str) -> impl Fn(&StrCol, RoaringBitmap) -> RoaringBitmap + '_ {
    move |metrics_col: &StrCol, idxs: RoaringBitmap| metrics_col.filter_with(idxs, &metric)
}

fn filter_by_tags<'a>(tags: &'a Tags) -> impl FnOnce(&TagCol, RoaringBitmap) -> RoaringBitmap + 'a {
    move |tag_col: &TagCol, idxs: RoaringBitmap| tag_col.filter_with(idxs, tags)
}

fn agg<'a>(aggs: &Vec<Agg>) -> impl FnOnce(&ValCol, RoaringBitmap) -> Option<Vec<f64>> + '_ {
    move |val_col: &ValCol, idx: RoaringBitmap| {
        let vals = val_col.get(&idx);

        aggs.iter()
            .map(|agg| match agg {
                Agg::Min => min!(vals),
                Agg::Max => max!(vals),
                Agg::Avg => avg!(vals),
                Agg::Sum => sum!(vals),
                Agg::Median => {
                    let mut vals = vals.clone();
                    median!(vals)
                }
            })
            .collect()
    }
}

struct Pstep<F1, F2, F3, F4> {
    time_filter: F1,
    metrics_filter: F2,
    tags_filter: F3,
    aggs: F4,
}

/// A query is executed in these steps:
/// 1. _Chunk up the time_: Divide the big start and finish interval into multiple chunks. The
///     aggregates are calculated over the se chunks. Then for each chunk:
///     1.a. Apply the metrics name filter and the tags filter in parallel.
///     1.b. Do an intersection of the above filters to identify the matching rows/ datapoints.
///     1.c Fetch the data points. The fetching for different columns of the datapoints can happen
///         in parallel.
///     1.d. Calculate the aggregate on the value column.
///     1.e Return the result of the chunk. Because this is based on streaming protocol, the
///         server should send data as they are available and the client can piece them together.
fn plan(
    qry: &Query,
) -> Vec<
    Pstep<
        impl FnOnce(&TsCol) -> RoaringBitmap + '_,
        impl Fn(&StrCol, RoaringBitmap) -> RoaringBitmap + '_,
        impl FnOnce(&TagCol, RoaringBitmap) -> RoaringBitmap + '_,
        impl FnOnce(&ValCol, RoaringBitmap) -> Option<Vec<f64>> + '_,
    >,
> {
    let start = qry.start_sec;
    let end = qry.end_sec;
    let sz = qry.chunk_sz_sec;

    let time_chunker = chunk_time(start, end, sz);

    let steps: Vec<_> = qry
        .metrics
        .iter()
        .flat_map(|metric| {
            time_chunker
                .iter()
                .map(|r| Pstep {
                    time_filter: filter_by_time(r.clone()),
                    metrics_filter: filter_by_metrics(&metric.name),
                    tags_filter: filter_by_tags(&metric.filters),
                    aggs: agg(&metric.agg),
                })
                .collect::<Vec<_>>()
        })
        .collect();
    steps
}

#[cfg(test)]
mod tests {
    use mem_col_store::col_tys::tag_col::{self, tags_from_str};
    use serde_json::json;
    use utils::compose;

    use super::*;

    #[test]
    fn t_chunk_time() {
        let x = chunk_time(0, 37, 7);
        assert_eq!(vec![0..7, 7..14, 14..21, 21..28, 28..35, 35..37], x);
        let x = chunk_time(0, 7, 10);
        assert_eq!(vec![0..7], x);
        let x = chunk_time(0, 70, 10);
        assert_eq!(
            vec![0..10, 10..20, 20..30, 30..40, 40..50, 50..60, 60..70],
            x
        );
    }

    #[test]
    fn test_filter_by_time() {
        let time_fl = super::filter_by_time(34..45);
        let ts_col = TsCol::with_range(20..40);
        let tts = time_fl(&ts_col);

        let mut rbm = RoaringBitmap::new();
        rbm.insert_range(14..20);

        assert_eq!(rbm, tts)
    }

    #[test]
    fn test_filter_by_metrics() {
        let metrics_col_strs = vec!["cpu", "memory", "gc", "cache", "gc", "net"];
        let metrics_col = StrCol::with_strs(metrics_col_strs.clone());
        let filter_by_metrics = super::filter_by_metrics("gc");

        let mut rbm = RoaringBitmap::new();
        rbm.insert(2);
        rbm.insert(1);

        assert_eq!(
            vec!["gc"],
            filter_by_metrics(&metrics_col, rbm)
                .iter()
                .map(|i| metrics_col_strs[i as usize])
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_filter_by_tags() {
        let tags_to_match = tags_from_str(
            &json!({
                "app": ["postgres", "zookeeper"]
            })
            .to_string(),
        )
        .unwrap();

        let tag_fl = super::filter_by_tags(&tags_to_match);

        let tag_col = tag_col::get_dummy_tag_col();

        let mut rbm = RoaringBitmap::new();
        rbm.insert(1);
        rbm.insert(3);

        let m = tag_fl(&tag_col, rbm);

        assert_eq!(vec![3], m.iter().collect::<Vec<_>>())
    }

    #[test]
    fn test_plan() {
        let js = json! ({
            "start_sec": 345,
            "end_sec": 400,
            "chunk_sz_sec": 10,
            "metrics" : [{
                "name": "cpu",
                "filters": {
                        "server" : ["A", "B", "c"],
                        "version": ["9.3", "10.0"],
                },
                "agg": ["median"],
            }, {
                "name": "mem",
                "agg": ["min", "max", "avg"]
            }]
        });

        let query: Query = serde_json::from_value(js).unwrap();
        let steps = plan(&query);
        assert_eq!(12, steps.len());

        let time_range = 300..350;

        let ts_col = TsCol::with_range(time_range.clone());

        let metrics_col_strs = vec!["cpu", "memory", "gc", "cache", "gc", "net"];
        let mut metrics_col = StrCol::new();
        time_range
            .clone()
            .step_by(metrics_col_strs.len())
            .for_each(|_| metrics_col.append(&metrics_col_strs));

        let tag1 = tags_from_str(
            &json!({
                "shard" : ["0", "59"],
                "version": ["8.4"],
                "app": ["postgres"],
                 "server" : ["A"]
            })
            .to_string(),
        )
        .unwrap();

        let tag2 = tags_from_str(
            &json!({
                "shard" : ["10", "599"],
                "version": ["8.234"],
                "app": ["light-ts"]
            })
            .to_string(),
        )
        .unwrap();

        let mut tag_col = TagCol::new();
        time_range.clone().for_each(|x| {
            if x % 2 == 0 {
                tag_col.append(tag1.clone())
            } else {
                tag_col.append(tag2.clone())
            }
        });

        let mut val_col = ValCol::new();
        let vals: Vec<_> = time_range.clone().map(|x| x as f64).collect();
        val_col.append(vals);

        let vals = steps.into_iter().filter_map(
            |Pstep {
                 time_filter,
                 metrics_filter,
                 tags_filter,
                 aggs,
             }| {
                 let vals =
                    compose!(time_filter(&ts_col) => metrics_filter(&metrics_col, _) => tags_filter(&tag_col, _) => aggs(&val_col, _));
                 vals
             },
        ).collect::<Vec<_>>();
        assert_eq!(vec![vec![348.0]], vals);
    }
}
