//! Timeseries analytical queries are like give me `the p99 latency for the runtime for the past
//! three days starting at 10 AM bucketised over 5 minute where the tags are application = postgres
//! and version = [2.3, 7.9]`. So given this query how will the plan for the same look like - ? Well
//! one approach can be determine where to start looking for the data. That part might be answered
//! by the ask for the past three days. So, I will start with the timestamp column just to get the
//! starting index. Now timeseries databases are huge and on a cloud native environment, they will
//! be stored in S3 or Google's Object store. So, first task will be to identify the right chunks to
//! download on to local disk. For this we have to rely on the object listing, if object names are
//! timestamped in some way or scan the mapping in Dynamo or Google Spanner. But say, we have
//! located the chunks where we start at some offset of the first chunk and then walk our way
//! forward. So, first we will have to figure out what that offset is. In timeseries, that question
//! will be answered by the timewindow of interest - here 10AM. Therefore, we must load the
//! timestamp column into memory from the disk and start filtering the rows that are before 10AM.
//! Once we know what index that is, we are ready to do a parallel scan. We are yet to determine the
//! endtime index. And once we have both (or start early and have the endpoint communicated to the
//! other workers later on). Once we have the indices, we can start parallel threads applying
//! filters on other columns as asked by the user : In this example, the latency filter must be
//! applied on the metric column and the tags filter must be applied on the tag column. But they can
//! happen in parallel. Each time the metrics thread blazes past the next 5 minutes of data, it
//! forwards the indices of the matching rows to the processor. Similarly the tags filter. Once the
//! processor has the data points for a given 5 minue bucket from all the filtering threads, it
//! applies the `intersection` filter on top and then passes the matching points over to the
//! aggregator. The aggregator calculates the value for the 5 min bucket and then lets the transport
//! know so that the server can push the result to the client. This is a streaming API, so the data
//! is made available to the client as they are available and the client can start
//! processing/plotting them. As you can see, everything starts with a filter. Filters are modelled
//! on the Rust Filter API so that functional nature and chaining can be supported and also multiple
//! filters can be applied in parallel. Filters are applied as close to data as possible.
//! Unfortunately, the current object stores don't provide arbitrary filtering and therefore, some
//! datamovement might happen. But then we must try to apply all the filters on top ofthe wire
//! protocol with zero copy to reduce as much overhead as possible. Because of applying the filters
//! as early as possible before doing any processing, this crate has a dependency on the storage
//! crate.

use std::fmt;
mod str_filter;


#[cfg(test)]
mod tests {
    #[test]
    fn t_filter() {

    }
}

