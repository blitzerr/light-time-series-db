use internment::ArcIntern;

pub mod errs;
pub mod search_based;
pub mod col_store;
pub mod col_tys;

pub const SOURCE_STR: &str = "source";
pub const METRIC_STR: &str = "metric";
pub const TIMESTAMP_STR: &str = "timestamp";
pub const VALUE_STR: &str = "value";
pub const TAGS_STR: &str = "tags";

type TagStrTy = ArcIntern<&'static str>;

/// Tags can be a simple key/value pair where value is a single value string.
/// This API also supports providing the value as a list of strings.
#[derive(Debug, Clone)]
pub enum TagTy {
    Str(TagStrTy, TagStrTy),
    List(TagStrTy, Vec<TagStrTy>),
}

#[derive(Debug)]
pub struct TimestampedVals {
    // The timestamp should be unix epoch format in milliseconds.
    timestamp: u64,
    // TODO: Extend this to include structured logs.
    value: f64,
    // Each data point can be associated with multiple tags.
    tags: Vec<TagTy>,
}

/// A point identifies a datapoint in a particular series. Usually, the clients like beats or
/// firelens tries to optimize for network throughput by batching points and therefore, the struct
/// is designed in a way that supports multiple points from the same series in a batch and then
/// many such series points can be batched on top to follow a cache friendly data-oriented design.
#[derive(Debug)]
pub struct Point {
    source: String,
    metric: String,
    vals: Vec<TimestampedVals>,
}

// A time series is an n-tuple defined by a metric and then the values are
// (timestamp, value, tags).
// A timestamp is a unix epoch type U64 value in milliseconds.
// Values are floating points or integers.
// Strings are key/value pairs where keys are strings and values can be string or lists.
// It makes sense for tags tobe lists because, say you want to store the CPU utilization
// metric for your hosts and say your application is a distributed database. You might want
// to tag the measurement with the shard that were hosted by the system at the time. So,
// the metric can be (in json format):
// {"source":"galaxy", "metric":"CPU", "ts":1627530262000, "value":93.3, "tags":{"app"="myDB", "version"="9.3", "shards"=[A,B]}}
