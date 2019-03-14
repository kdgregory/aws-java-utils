# Change History

## 1.11.0 (TBD)

* `CloudWatchLogsReader`, for reading from one or more CloudWatch Logs streams without
   having to think about pagination.
* `CloudWatchLogsWriter`, for writing to a CloudWatch Logs stream, either under direct
  program control or via a scheduled backgound thread.
* `CloudWatchLogsUtil`, a collection of static utility methods for working with CloudWatch
  Logs resources.
* `KinesisReader`, for restartable writing from Kinesis streams, accounting for resharding.
* `KinesisWriter`, for writing to Kinesis streams in batches, possibly on a background thread.
* `KinesisUtil`, a collection of static utility methods for working with Kinesis streams.
* `MetricReporter`, for reporting metrics to CloudWatch.
