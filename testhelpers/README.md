# AWS Test Helpers

This module contains mock objects and other utility classes used in testing the main
library. They may be useful for other projects.


## Mock Objects

Mock objects allow test code to verify the interaction of the library with the AWS SDK,
without the cost and time of creating actual AWS resources. They also allow simulating
failure conditions that may be very difficult to re-create consistently. Against this,
a mock object embeds one's possibly-incorrect assumptions about how the actual service
behaves, mock-object tests must be backed up by integration tests that validate against
the actual SDK.

In working with AWS services, I have found that an actual mock class is superior to a
mock-object framework: the latter tends to require a lot of boilerplate for non-trivial
interactions. However, mocking complete services would be a signficant project by itself;
I wouldn't have time to actually implement the utility library.

My solution is to use [reflection proxies](https://www.kdgregory.com/index.php?page=junit.proxy),
with a twist: the mock object class has to implement any methods that it wants to mock.
This simplifies the invocation handler quite a bit: rather than a long if-else chain
that checks method name and parameters, you can simply invoke the provided `Method`
object on `this`.

The invocation handler also captures the number of times each method was invoked, and the
parameters passed in those invocations. You access these with the `getInvocationCount()`,
`getLastCallParameters()`, and `getAllCallParameters()` methods, passing the name of the
mocked method.

The default mock implementations provide "reasonable" return values, typically controlled
via some configuration values. See the JavaDoc for a particular mock object for more info.
