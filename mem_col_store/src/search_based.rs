use std::{ops::Range, path::Path, u64};

use crate::{errs::ColStoreError, Point, METRIC_STR, SOURCE_STR, TIMESTAMP_STR, VALUE_STR};
use color_eyre::eyre::Result;
use tantivy::collector::FilterCollector;

use tantivy::{
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{Cardinality, IntOptions, Schema, TextFieldIndexing, TextOptions},
    DocAddress, Index, IndexWriter, Opstamp, ReloadPolicy,
};

struct SearchBasedWriter {
    index: Index,
    i_writer: IndexWriter,
    schema: Schema,
}

impl SearchBasedWriter {
    fn create_idx(index_path: &Path) -> Result<(Index, Schema)> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field(
            SOURCE_STR,
            TextOptions::default()
                .set_stored()
                .set_indexing_options(TextFieldIndexing::default()),
        );
        schema_builder.add_text_field(
            METRIC_STR,
            TextOptions::default()
                .set_stored()
                .set_indexing_options(TextFieldIndexing::default()),
        );
        schema_builder.add_u64_field(
            TIMESTAMP_STR,
            IntOptions::default()
                .set_stored()
                .set_indexed()
                .set_fast(Cardinality::SingleValue),
        );
        schema_builder.add_f64_field(
            VALUE_STR,
            IntOptions::default()
                .set_indexed()
                .set_stored()
                .set_fast(Cardinality::SingleValue),
        );

        // TODO: Need to add the tags.
        let schema = schema_builder.build();
        let index = Index::create_in_dir(&index_path, schema.clone())?;
        Ok((index, schema))
    }

    pub(crate) fn new(index_path: &Path) -> Result<SearchBasedWriter> {
        let (index, schema) = SearchBasedWriter::create_idx(index_path)?;
        let i_writer = index.writer(50_000_000)?;
        Ok(SearchBasedWriter {
            index,
            i_writer,
            schema,
        })
    }

    pub(crate) fn put_stream(&self, batch: Vec<Point>) -> Result<()> {
        let source_field = self
            .schema
            .get_field(SOURCE_STR)
            .ok_or(ColStoreError::InvalidField(SOURCE_STR))?;
        let metric_field = self
            .schema
            .get_field(METRIC_STR)
            .ok_or(ColStoreError::InvalidField(METRIC_STR))?;
        let timestamp_field = self
            .schema
            .get_field(TIMESTAMP_STR)
            .ok_or(ColStoreError::InvalidField(TIMESTAMP_STR))?;
        let value_field = self
            .schema
            .get_field(VALUE_STR)
            .ok_or(ColStoreError::InvalidField(VALUE_STR))?;

        batch.iter().for_each(|point| {
            let source = point.source.as_str();
            let metric = point.metric.as_str();
            point.vals.iter().for_each(|val| {
                let doc = doc!(
                source_field => source,
                metric_field => metric,
                timestamp_field => val.timestamp,
                value_field => val.value,
                );
                self.i_writer.add_document(doc);
            });
        });

        Ok(())
    }
    pub(crate) fn close_stream(&mut self) -> Result<Opstamp> {
        let op_stamp = self.i_writer.commit()?;
        Ok(op_stamp)
    }

    pub(crate) fn query(
        index: &Index,
        schema: &Schema,
        range: Range<u64>,
        metrics: &Vec<&str>,
    ) -> Result<Vec<Point>> {
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        let metric_field = schema
            .get_field(METRIC_STR)
            .ok_or(ColStoreError::InvalidField(METRIC_STR))?;
        let searcher = reader.searcher();

        let ts_field = schema
            .get_field(TIMESTAMP_STR)
            .ok_or(ColStoreError::InvalidField(TIMESTAMP_STR))?;

        let collector = FilterCollector::new(
            ts_field,
            move |ts: u64| ts > range.start && ts < range.end,
            TopDocs::with_limit(10).order_by_fast_field(ts_field),
        );

        let query_parser = QueryParser::for_index(&index, vec![metric_field]);

        let docs: Result<Vec<String>> = metrics
            .iter()
            .filter_map(|&m| query_parser.parse_query(m).ok())
            .filter_map(|q| searcher.search(&q, &collector).ok())
            .flatten()
            .map(|(_fast_f, doc_address): (u64, DocAddress)| {
                searcher
                    .doc(doc_address)
                    .map_err(|e| e.into())
                    .map(|x| schema.to_json(&x))
            })
            .collect();
        println!("{:?}", &docs?);
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use core::f64;
    use std::{path::Path, u64, vec};

    use super::SearchBasedWriter;
    use crate::{Point, TimestampedVals};

    #[test]
    fn t_index() {
        let idx_path = Path::new("/tmp/light_tsdb/");

        // Delete and recreate the directory if already exists or it will throw index already exists
        // exception.
        if std::fs::remove_dir_all(idx_path).is_ok() {
            std::fs::create_dir(idx_path).unwrap();
        }
        std::fs::create_dir(idx_path);

        let mut writer = SearchBasedWriter::new(idx_path).unwrap();
        let points = gen_points(
            &vec!["server", "database"],
            &vec!["cpu", "mem"],
            1627753205000,
            1627753205007,
        );
        writer.put_stream(points).unwrap();
        let op = writer.close_stream().unwrap();

        println!("operation: {}", op);
        SearchBasedWriter::query(
            &writer.index,
            &writer.schema,
            1627753205000..1627753205005,
            &vec!["cpu"],
        )
        .unwrap();
    }

    fn gen_points(sources: &Vec<&str>, metric: &Vec<&str>, start: u64, end: u64) -> Vec<Point> {
        assert_eq!(
            sources.len(),
            metric.len(),
            "expected sources and metrics to be of same lengths. Provided, sources: {:?}, meric: {:?}",
            sources,
            metric
        );

        sources
            .iter()
            .zip(metric)
            .map(|(s, m)| {
                let ts_vals: Vec<TimestampedVals> = (start..end)
                    .map(|t| TimestampedVals {
                        timestamp: t,
                        value: t as f64,
                        _tags: vec![],
                    })
                    .collect();
                Point {
                    source: s.to_owned().to_string(),
                    metric: m.to_owned().to_string(),
                    vals: ts_vals,
                }
            })
            .collect()
    }
}
