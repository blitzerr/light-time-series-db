use mem_col_store::col_tys::tag_col::Tags;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub enum Agg {
    #[serde(rename = "min")]
    Min,
    #[serde(rename = "max")]
    Max,
    #[serde(rename = "avg")]
    Avg,
    #[serde(rename = "sum")]
    Sum,
    #[serde(rename = "median")]
    Median,
}

#[derive(Debug, Deserialize, Clone)]
pub struct QMetric {
    pub name: String,
    #[serde(default)]
    pub filters: Tags,
    pub agg: Vec<Agg>,
}

#[derive(Debug, Deserialize)]
pub struct Query {
    /// The metrics emitted during this time window will be considered.
    pub start_sec: u64,
    pub end_sec: u64,
    /// The specified time_window will be partitioned into smaller chunks of this size before doing
    /// aggregations on them.
    pub chunk_sz_sec: u64,
    pub metrics: Vec<QMetric>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::qry::Query;

    #[test]
    fn t_deser() {
        let js = json! ({
            "start_sec": 345,
            "end_sec": 567,
            "chunk_sz_sec": 5,
            "metrics" : [{
                "name": "cpu",
                "filters": {
                        "server" : ["A", "B", "c"],
                        "version": ["9.3", "10.0"],
                },
                "agg": ["median"],
            }, {
                "name": "mem",
                "agg": ["min", "max", "avg"],
            }]
        });

        let query: Query = serde_json::from_value(js).unwrap();
        println!("{:?}", query);
    }
}
