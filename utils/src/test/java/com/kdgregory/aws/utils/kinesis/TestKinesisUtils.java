// Copyright Keith D Gregory
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.kdgregory.aws.utils.kinesis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import static org.junit.Assert.*;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.test.SelfMock;
import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  Mock-object tests of KinesisUtils.
 */
public class TestKinesisUtils
{
//----------------------------------------------------------------------------
//  Sample data -- only populated with fields that we actually use
//----------------------------------------------------------------------------

    private static final List<Shard> SHARDS_1 = Arrays.asList(
                                        new Shard().withShardId("0001"),
                                        new Shard().withShardId("0002"));
    private static final List<Shard> SHARDS_2 = Arrays.asList(
                                        new Shard().withShardId("0003"),
                                        new Shard().withShardId("0004"));

//----------------------------------------------------------------------------
//  Test Helpers
//----------------------------------------------------------------------------

    /**
     *  A base mock class that returns a sequence of values for describeStream.
     *  Constructed with a list of status values and/or exception classes, and
     *  will return one value for each call. Calls that exceed the number of
     *  values provided will reuse the last value.
     *  <p>
     *  For more complex behaviors, override {@link addToDescription}.
     */
    private static class StreamDescriberMock extends SelfMock<AmazonKinesis>
    {
        public AtomicInteger describeInvocationCount = new AtomicInteger(0);

        private String expectedStreamName;
        private Object[] statuses;

        public StreamDescriberMock(String expectedStreamName, Object... statuses)
        {
            super(AmazonKinesis.class);
            this.expectedStreamName = expectedStreamName;
            this.statuses = statuses;
        }

        @SuppressWarnings("unused")
        public DescribeStreamResult describeStream(DescribeStreamRequest request)
        {
            assertEquals("request contains stream name", expectedStreamName, request.getStreamName());

            int idx = describeInvocationCount.getAndIncrement();
            Object retval = (idx < statuses.length) ? statuses[idx] : statuses[statuses.length - 1];
            if (retval == ResourceNotFoundException.class)
            {
                throw new ResourceNotFoundException("");
            }
            else if (retval == LimitExceededException.class)
            {
                throw new LimitExceededException("");
            }
            else
            {
                StreamDescription description = new StreamDescription()
                        .withStreamName(request.getStreamName())
                        .withStreamStatus((StreamStatus)retval);
                addToDescription(request, description);
                return new DescribeStreamResult().withStreamDescription(description);
            }
        }

        protected void addToDescription(DescribeStreamRequest request, StreamDescription description)
        {
            // default implementation does nothing
        }
    }


    /**
     *  A mock object for testing the increase/decrease retention period calls. Tracks
     *  the number of invocations, returns a sequence of status results with current
     *  retention period, and allows override of the call behavior.
     */
    private static class RetentionPeriodMock extends StreamDescriberMock
    {
        public AtomicInteger currentRetentionPeriod = new AtomicInteger(0);
        public AtomicInteger increaseInvocationCount = new AtomicInteger(0);
        public AtomicInteger decreaseInvocationCount = new AtomicInteger(0);

        private String expectedStreamName;
        private int expectedRetentionPeriod;

        public RetentionPeriodMock(String expectedStreamName, int startingRetentionPeriod, int expectedRetentionPeriod, Object... statuses)
        {
            super(expectedStreamName, statuses);
            this.expectedStreamName = expectedStreamName;
            this.expectedRetentionPeriod = expectedRetentionPeriod;
            this.currentRetentionPeriod.set(startingRetentionPeriod);
        }

        @Override
        protected void addToDescription(DescribeStreamRequest request, StreamDescription description)
        {
            description.setRetentionPeriodHours(currentRetentionPeriod.get());
        }

        @SuppressWarnings("unused")
        public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest request)
        {
            increaseInvocationCount.getAndIncrement();
            assertEquals("stream name passed to increaseStreamRetentionPeriod",     expectedStreamName,      request.getStreamName());
            assertEquals("retntion period passed to increaseStreamRetentionPeriod", expectedRetentionPeriod, request.getRetentionPeriodHours().intValue());
            increaseStreamRetentionPeriodInternal(request);
            return new IncreaseStreamRetentionPeriodResult();
        }

        protected void increaseStreamRetentionPeriodInternal(IncreaseStreamRetentionPeriodRequest request)
        {
            currentRetentionPeriod.set(request.getRetentionPeriodHours());
        }

        @SuppressWarnings("unused")
        public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest request)
        {
            decreaseInvocationCount.getAndIncrement();
            assertEquals("stream name passed to decreaseStreamRetentionPeriod",     expectedStreamName,      request.getStreamName());
            assertEquals("retntion period passed to decreaseStreamRetentionPeriod", expectedRetentionPeriod, request.getRetentionPeriodHours().intValue());
            decreaseStreamRetentionPeriodInternal(request);
            return new DecreaseStreamRetentionPeriodResult();
        }

        protected void decreaseStreamRetentionPeriodInternal(DecreaseStreamRetentionPeriodRequest request)
        {
            currentRetentionPeriod.set(request.getRetentionPeriodHours());
        }
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testDescribeShardsSingleRetrieve() throws Exception
    {
        StreamDescriberMock mock = new StreamDescriberMock("example", StreamStatus.ACTIVE)
        {
            @Override
            protected void addToDescription(DescribeStreamRequest request, StreamDescription description)
            {
                description.withShards(SHARDS_1).withHasMoreShards(Boolean.FALSE);
            }
        };
        AmazonKinesis client = mock.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("invocation count",        1, mock.describeInvocationCount.get());
        assertEquals("returned expected list",  SHARDS_1, shards);
    }


    @Test
    public void testDescribeShardsMultiRetrieve() throws Exception
    {
        StreamDescriberMock mock = new StreamDescriberMock("example", StreamStatus.ACTIVE)
        {
            @Override
            protected void addToDescription(DescribeStreamRequest request, StreamDescription description)
            {
                if ("0002".equals(request.getExclusiveStartShardId()))
                {
                    description.withShards(SHARDS_2).withHasMoreShards(Boolean.FALSE);
                }
                else
                {
                    description.withShards(SHARDS_1).withHasMoreShards(Boolean.TRUE);
                }
            }
        };
        AmazonKinesis client = mock.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("invocation count",        2, mock.describeInvocationCount.get());
        assertEquals("returned expected list",  CollectionUtil.combine(new ArrayList<Shard>(), SHARDS_1, SHARDS_2), shards);
    }


    @Test
    public void testDescribeShardsStreamNotAvailable() throws Exception
    {
        AmazonKinesis client = new StreamDescriberMock("example", ResourceNotFoundException.class)
                               .getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned empty", null, shards);
    }


    @Test
    public void testDescribeShardsRequestThrottling() throws Exception
    {
        final List<Shard> expected = CollectionUtil.combine(new ArrayList<Shard>(), SHARDS_1, SHARDS_2);
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                if (invocationCount.getAndIncrement() % 2 == 0)
                {
                    throw new LimitExceededException("");
                }

                if ("0002".equals(request.getExclusiveStartShardId()))
                {
                    return new DescribeStreamResult().withStreamDescription(
                            new StreamDescription().withShards(SHARDS_2).withHasMoreShards(Boolean.FALSE));
                }
                else
                {
                    return new DescribeStreamResult().withStreamDescription(
                            new StreamDescription().withShards(SHARDS_1).withHasMoreShards(Boolean.TRUE));
                }
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned expected list", expected, shards);
        assertEquals("number of calls", 4, invocationCount.get());
    }


    @Test
    public void testDescribeShardsTimoutExceeded() throws Exception
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                // we'll return one batch but then pretend to be throttled
                if (invocationCount.getAndIncrement() > 0)
                {
                    throw new LimitExceededException("");
                }

                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withShards(SHARDS_1).withHasMoreShards(Boolean.TRUE));
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 150);
        assertEquals("did not return anything", null, shards);
        assertEquals("number of calls", 3, invocationCount.get());
    }


    @Test
    public void testWaitForStatusNormalOperation()
    {
        StreamDescriberMock mock = new StreamDescriberMock("example", StreamStatus.CREATING, StreamStatus.CREATING, StreamStatus.ACTIVE);
        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, "example", StreamStatus.ACTIVE, 500);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.ACTIVE, lastStatus);
        assertEquals("invocation count",    3, mock.describeInvocationCount.get());
        assertApproximate("elapsed time",   200, elapsed, 10);
    }


    @Test
    public void testWaitForStatusTimeout()
    {
        StreamDescriberMock mock = new StreamDescriberMock("example", StreamStatus.CREATING);
        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, "example", StreamStatus.ACTIVE, 250);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.CREATING, lastStatus);
        assertEquals("invocation count",    3, mock.describeInvocationCount.get());
        assertApproximate("elapsed time",   300, elapsed, 10);
    }


    @Test
    public void testWaitForStatusWithThrottling()
    {
        StreamDescriberMock mock = new StreamDescriberMock("example",
                                                           LimitExceededException.class, LimitExceededException.class,
                                                           StreamStatus.CREATING, StreamStatus.ACTIVE);
        AmazonKinesis client = mock.getInstance();

        // the expected sequence of calls:
        //  - limit exceeded, sleep for 100 ms
        //  - limit exceeded, sleep for 200 ms
        //  - return creating, sleep for 100 ms
        //  - return active, no sleep

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, "example", StreamStatus.ACTIVE, 500);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.ACTIVE, lastStatus);
        assertEquals("invocation count",    4, mock.describeInvocationCount.get());
        assertApproximate("elapsed time",   400, elapsed, 10);
    }


    @Test
    public void testCreateStreamHappyPath() throws Exception
    {
        final AtomicInteger createInvocationCount = new AtomicInteger(0);

        StreamDescriberMock mock = new StreamDescriberMock("example", ResourceNotFoundException.class, StreamStatus.CREATING, StreamStatus.ACTIVE)
        {
            @SuppressWarnings("unused")
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                assertEquals("stream name passed to createStream", "example", request.getStreamName());
                assertEquals("shard count passed to createStream", 3,         request.getShardCount().intValue());
                createInvocationCount.incrementAndGet();
                return new CreateStreamResult();
            }
        };
        AmazonKinesis client = mock.getInstance();

        // the expected sequence of calls:
        //  - resource not found, sleep for 100 ms
        //  - creating, sleep for 100 ms
        //  - active, no sleep

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.createStream(client, "example", 3, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("stream status",                   StreamStatus.ACTIVE, status);
        assertEquals("invocations of createStream",     1, createInvocationCount.get());
        assertEquals("invocations of describeStream",   3, mock.describeInvocationCount.get());
        assertApproximate("elapsed time",               200, elapsed, 10);
    }


    @Test
    public void testCreateStreamThrottling() throws Exception
    {
        final AtomicInteger createInvocationCount = new AtomicInteger(0);

        StreamDescriberMock mock = new StreamDescriberMock("example", StreamStatus.ACTIVE)
        {
            @SuppressWarnings("unused")
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                if (createInvocationCount.getAndIncrement() < 2)
                    throw new LimitExceededException("");
                else
                    return new CreateStreamResult();
            }
        };
        AmazonKinesis client = mock.getInstance();

        // expected calls:
        //  - throttled, sleep for 100 ms
        //  - throttled, sleep for 200 ms
        //  - success

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.createStream(client, "example", 3, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("stream status",                               StreamStatus.ACTIVE, status);
        assertApproximate("elapsed time",                           300, elapsed, 10);
        assertEquals("invocations of createStream",                 3, createInvocationCount.get());
        assertEquals("invocations of describeStream",               1, mock.describeInvocationCount.get());
    }


    @Test
    public void testCreateStreamAlreadyExists() throws Exception
    {
        final AtomicInteger createInvocationCount = new AtomicInteger(0);

        StreamDescriberMock mock = new StreamDescriberMock("example", StreamStatus.ACTIVE)
        {
            @SuppressWarnings("unused")
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                createInvocationCount.getAndIncrement();
                throw new ResourceInUseException("");
            }
        };
        AmazonKinesis client = mock.getInstance();

        StreamStatus status = KinesisUtils.createStream(client, "example", 3, 500L);

        assertEquals("stream status",                               StreamStatus.ACTIVE, status);
        assertEquals("invocations of createStream",                 1, createInvocationCount.get());
        assertEquals("invocations of describeStream",               1, mock.describeInvocationCount.get());
    }


    @Test
    public void testDeleteStreamHappyPath() throws Exception
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DeleteStreamResult deleteStream(DeleteStreamRequest request)
            {
                invocationCount.getAndIncrement();
                assertEquals("stream name passed in request", "example", request.getStreamName());
                return new DeleteStreamResult();
            }
        }.getInstance();

        boolean status = KinesisUtils.deleteStream(client, "example", 500L);

        assertTrue("returned status indicates success",         status);
        assertEquals("invocations of deleteStream",             1, invocationCount.get());
    }


    @Test
    public void testDeleteStreamThrottling() throws Exception
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DeleteStreamResult deleteStream(DeleteStreamRequest request)
            {
                invocationCount.getAndIncrement();
                throw new LimitExceededException("");
            }
        }.getInstance();

        // expected calls:
        //  - throttled, sleep for 100 ms
        //  - throttled, sleep for 200 ms

        long start = System.currentTimeMillis();
        boolean status = KinesisUtils.deleteStream(client, "example", 250L);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse("returned status indicates failure",        status);
        assertApproximate("elapsed time",                       300, elapsed, 10);
        assertEquals("invocations of deleteStream",             2, invocationCount.get());
    }


    @Test
    public void testDeleteStreamDoesntExist() throws Exception
    {
        final AtomicInteger invocationCount = new AtomicInteger(0);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DeleteStreamResult deleteStream(DeleteStreamRequest request)
            {
                invocationCount.getAndIncrement();
                throw new ResourceNotFoundException("");
            }
        }.getInstance();

        boolean status = KinesisUtils.deleteStream(client, "example", 250L);

        assertFalse("returned status indicates failure",        status);
        assertEquals("invocations of deleteStream",             1, invocationCount.get());
    }


    @Test
    public void testUpdateRetentionPeriodIncreaseHappyPath() throws Exception
    {
        RetentionPeriodMock mock = new RetentionPeriodMock("example", 24, 36, StreamStatus.ACTIVE, StreamStatus.UPDATING, StreamStatus.ACTIVE)
        {
            @Override
            protected void decreaseStreamRetentionPeriodInternal(DecreaseStreamRetentionPeriodRequest request)
            {
                throw new UnsupportedOperationException("decreaseStreamRetentionPeriod should not be called by this test");
            }
        };

        AmazonKinesis client = mock.getInstance();

        // the expected sequence of calls:
        //  - initial describe
        //  - increase retention
        //  - updating, sleep for 100 ms
        //  - active

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.updateRetentionPeriod(client, "example", 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.currentRetentionPeriod.get());
        assertEquals("invocations of describeStream",                   3,  mock.describeInvocationCount.get());
        assertEquals("invocations of increaseStreamRetentionPeriod",    1,  mock.increaseInvocationCount.get());
        assertApproximate("elapsed time",                               100, elapsed, 10);
    }


    @Test
    public void testUpdateRetentionPeriodDecreaseHappyPath() throws Exception
    {
        RetentionPeriodMock mock = new RetentionPeriodMock("example", 48, 36, StreamStatus.ACTIVE, StreamStatus.UPDATING, StreamStatus.ACTIVE)
        {
            @Override
            protected void increaseStreamRetentionPeriodInternal(IncreaseStreamRetentionPeriodRequest request)
            {
                throw new UnsupportedOperationException("increaseStreamRetentionPeriod should not be called by this test");
            }
        };

        AmazonKinesis client = mock.getInstance();

        // the expected sequence of calls:
        //  - initial describe
        //  - decrease retention
        //  - updating, sleep for 100 ms
        //  - active

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.updateRetentionPeriod(client, "example", 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.currentRetentionPeriod.get());
        assertEquals("invocations of describeStream",                   3,  mock.describeInvocationCount.get());
        assertEquals("invocations of decreaseStreamRetentionPeriod",    1,  mock.decreaseInvocationCount.get());
        assertApproximate("elapsed time",                               100, elapsed, 10);
    }
}
