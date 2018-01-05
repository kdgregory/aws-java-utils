# aws-utils

A collection of utility classes that operate at a slightly higher level than the AWS Java SDK.

For example, AWS "describe" operations are paginated: each call may return a token indicating
that there are more records, and you have to keep repeating the call until the returned token
is null. All the while being prepared to handle rate-limiting or other exceptions.


## Package structure

This library contains two root packages: `com.kdgregory.aws.utils` and `com.kdgregory.aws.test`.
Under these root packages are per-service packages, named according to the service package in
the AWS SDK.

Each of the `utils` service packages will contain at least one class, `FooUtil`, which provides
static utility mthods for service Foo. There may also be additional instantiable classes.

The `test` packages contain classes intended to support testing of client code that uses these
services; they primarily hold mock objects.

> This package structure may change; in particular, the test packages may be moved into their
  own library.


## Versioning

I follow the standard `MAJOR.MINOR.PATCH` versioning scheme:

* `MAJOR` tracks the AWS SDK minor version (eg, version 11.0.0 of this library is used with version 1.11.x of the SDK)
* `MINOR` is incremented whenever new services are supported; each AWS service typically has a single `FooUtils` class.
* `PATCH` is incremented for bugfixes and whenever new methods are added to an existing class.
  
Not all versions will be released to Maven Central. I may choose to make release (non-snapshot) versions for
development testing, or as interim steps of a bigger piece of functionality. However, all release versions
are tagged in source control, whether or not available on Maven Central.


## Dependencies

To avoid dependency hell, this project does not use third-party dependencies, even
those that I've written. Moreover, all AWS SDK dependencies are marked as `provided`;
your project must include those dependencies explicitly.

The project is built for JDK 6; it does not use any features from later Java versions.
As of version XXX, the AWS SDK also supports JDK 6, and all integration tests run on an
OpenJDK system. However, there is no guarantee that the AWS SDK will remain compatible.


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
