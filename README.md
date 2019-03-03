# aws-utils

A collection of utility classes that operate at a higher level than the AWS Java SDK.


## Directory structure

This project contains three directories:

* `library` is the main library of utility classes.
* `testhelpers` contains mock objects and other classes to support testing the library. This
  will be published in case it's useful for anyone else.
* `examples` contains programs that demonstrate the library.
* `integration-tests` are tests that exercise the library classes against actual AWS services.
  *Beware:* running these tests will incur service charges.


## Package Structure

The top-level package is `com.kdgregory.aws.utils`. Under this are packages named after AWS
services (eg: `com.kdgregory.aws.utils.kinesis`).

Within each package there is a class containing static utility methods, named after the service
(example: `KinesisUtil`). There may also be instantiable classes (example: `KinesisReader`).


## Versioning

I follow the standard `MAJOR.MINOR.PATCH` versioning scheme:

* `MAJOR` tracks the AWS SDK major version (currently only version 1 is supported, not version 2).
* `MINOR` tracks the AWS SDK minor version (currently 11).
* `PATCH` is incremented whenever functionality is added.
  
Not all versions will be released to Maven Central. I may choose to make release (non-snapshot) versions for
development testing, or as interim steps of a bigger piece of functionality. However, all release versions
are tagged in source control, whether or not available on Maven Central.


## Source Control

The `master` branch is intended for "potentially releasable" versions that correspond to
the current SDK minor release. Commits on master are functional, but may not be "complete"
(for some definition of that word). They may be "snapshot" or release builds. Master will
never be rebased; once a commit is made there it's part of history for better or worse.

Development takes place on a `dev-MAJOR.MINOR.PATCH` branch; these branches are deleted
once their content has been merged into `master`. *BEWARE*: these branches may be rebased
as I see fit.

Each "release" version is tagged with `release-MAJOR.MINOR.PATCH`, whether or not it was
uploaded to Maven Central.


## Dependencies

To avoid dependency hell, this project avoids third-party dependencies other than
those that are required by the AWS SDK (in particular, `commons-logging`, see below).
Furthermore, all dependencies are marked as `provided`, so will use the versions that
your project specifies.

Note that some functionality depends on specific AWS versions. These versions are
explicitly called out in the documentation, and using the feature with an earlier
AWS SDK will result in a `NoSuchMethodError`. As long as you do not actually call
such functions, however, you will be able to build and run with earlier versions
of the SDK.

The project produces classfiles compatible with Java 6, and does not use any features
from later Java versions. However, it has not been tested with a Java 6 environment,
and there is no guarantee that the AWS SDK will not rely on a later version.


## Operational Notes

# AWS Service Clients

All operations and classes must be provided with an appropriate AWS client. Per AWS docs,
clients are intended to be long-lived, and shared between multiple threads.


# Logging

This library uses [Apache commons-logging](http://commons.apache.org/proper/commons-logging/)
version 1.1.3, which is also a dependency of the AWS SDK. If you exlude the SDK's transitive
dependency and do not provide a replacement (such as `jcl-over-slf4j`), some operations will
throw a `NoClassDefFoundError`.

Operations use debug-level logging to report actions such as creating or deleting an AWS
resource, and warn-level logging to report any unexpected conditions (such as a timeout
expiring before a resource becomes ready). The library does not use info- or error-level
logging, and does not log normal operations (such as writing batches of messages).

All log messages are reported using the class name as a logger name. Using a hierarchical
logging framework such as Log4J, you can turn off all logging with the top-level package
name:

```
log4j.logger.com.kdgregory.aws.utils=OFF
```


# Exceptions

Where appropriate, this library catches exceptions thrown by the SDK. For example, when
writing to CloudWatch Logs the sequence token may be invalidated by a concurrent writer.
In this case the library can transparently retry the operation.

However, many exceptions are non-recoverable, and are allowed to propagate. Your code
should always be prepared to handle exceptions thrown by the underlying APIs.


# Batching and Background Execution

Most operations are intended to be called synchronously: the calling thread will wait
until the operation completes. In some cases, particularly classes that write to the
service, it is more efficient to batch operations and invoke them asynchronously.

Classes that support background operation provide a constructor that takes  a Java
`ExecutorService` instance; the intention is that the caller will create a shared
executor that will service multiple classes. The number of threads managed by this
executor will depend on the number and frequency of background operations, but in
general you don't need many threads: each operation will take 50-100 ms.

For classes that support batched operations, the executor must be an instance of
`ScheduledExecutorService`, and the class will also be constructed with a scheduling
interval. The library class will perform some action (typically writing a batch of
messages) every time the interval expires. If there is nothing to do, the background
operation will still run but won't contact the service.

In either case, if you shut down the executor service you may prevent any outstanding
operations from completing. Library classes that support background operation provide
a `shutdown()` method, which will attempt to complete any pending operations and then
prevent future operations from being submitted.

This caveat also applies to the application as a whole: if you call `System.exit()`
while operations are queued, they will never complete. You can add a shutdown hook
to guard against this case, but you can't guard against unexpected JVM termination.
You will need to determine whether the trade-off between efficiency and certainty
is appropriate for your application.
