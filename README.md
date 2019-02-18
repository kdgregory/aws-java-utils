# aws-utils

A collection of utility classes that operate at a slightly higher level than the AWS Java SDK.

For example, AWS "describe" operations are paginated: each call may return a token indicating
that there are more records, and you have to keep repeating the call until the returned token
is null. All the while being prepared to handle rate-limiting or other exceptions.


## Directory structure

This project contains three directories:

* `library` is the main library of utility classes.
* `examples` contains programs that demonstrate the library.
* `integration-tests` are tests that exercise the library classes against actual AWS services.
  *Beware:* running these tests will incur service charges.


## Package Structure

The top-level package is `com.kdgregory.aws.utils`. Under this are packages named after AWS
services (example: `com.kdgregory.aws.utils.kinesis`).

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


## Dependencies

To avoid dependency hell, this project avoids third-party dependencies other than
those that are required by the AWS SDK (in particular, `commons-logging`; see
below). Furthermore, all dependencies are marked as `provided`, so will use the
versions that your project includes.

Note that some functionality depends on specific AWS versions. These versions are
explicitly called out in the documentation, and using the feature with an earlier
AWS SDK will result in a `NoSuchMethodError`. As long as you do not actually call
such functions, however, you will be able to build and run with earlier versions
of the SDK.

The project is built for JDK 6; it does not use any features from later Java versions.
As of version XXX, the AWS SDK also supports JDK 6, and all integration tests have been
run on an OpenJDK 1.6 system. However, there is no guarantee that the AWS SDK will remain
compatible, and later versions may.


### commons-logging

This library uses [Apache commons-logging](http://commons.apache.org/proper/commons-logging/)
version 1.1.3, which is also a dependency of the AWS SDK. It is not designed to recognize
that the commons-logging JAR is not available in the classpath (neither does the SDK), so
if you don't like commons-logging you need to replace it (for example, with `jcl-over-slf4j`
if you use the SLF4J logging framework). _Unlike_ the SDK, you don't have to explicitly
ignore the transitive dependency.

This library does a moderate amount of debug-level logging, along with extensive logging of
error conditions. You probably don't want to see the former, but should enable the latter.
To do so with Log4J 1.x, use the following in your `log4j.properties` (adapt as needed for
other logging frameworks):

```
log4j.logger.com.kdgregory.aws.utils=ERROR
```


## Source Control

The `master` branch is intended for "potentially releasable" versions that correspond to
the current SDK minor release. Commits on master are functional, but may not be "complete"
(for some definition of that word). They may be "snapshot" or release builds. Master will
never be rebased; once a commit is made there it's part of history for better or worse.

Previous AWS SDK versions, where supported, will have a long-lived `support-MAJOR` branch.
New features will be backported to these branches.

Development takes place on a `dev-MAJOR.MINOR.PATCH` branch; these branches are deleted
once their content has been merged into `master`. *BEWARE*: these branches may be rebased
as I see fit.

Each "release" version is tagged with `release-MAJOR.MINOR.PATCH`, whether or not it was
uploaded to Maven Central.
