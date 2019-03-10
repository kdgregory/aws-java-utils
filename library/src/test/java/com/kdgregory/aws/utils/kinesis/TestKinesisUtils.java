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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.test.NumericAsserts;
import net.sf.kdgcommons.test.SelfMock;
import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  Mock-object tests of KinesisUtils.
 */
public class TestKinesisUtils
{
    private LinkedList<LoggingEvent> loggingEvents = new LinkedList<LoggingEvent>();

//----------------------------------------------------------------------------
//  Common setup
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        Logger utilsLogger = Logger.getLogger(KinesisUtils.class);
        utilsLogger.setLevel(Level.DEBUG);
        utilsLogger.addAppender(new AppenderSkeleton()
        {
            @Override
            public void close()
            {
                // no-op
            }

            @Override
            public boolean requiresLayout()
            {
                return false;
            }

            @Override
            protected void append(LoggingEvent event)
            {
                loggingEvents.add(event);
            }
        });
    }

//----------------------------------------------------------------------------
//  Sample data -- only populated with fields that we actually use
//----------------------------------------------------------------------------

    private static final Date NOW = new Date();

    public final static String STREAM_NAME = "TestKinesisUtils";

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

        protected String expectedStreamName;
        protected Object[] statuses;

        public StreamDescriberMock(String expectedStreamName, Object... statuses)
        {
            super(AmazonKinesis.class);
            this.expectedStreamName = expectedStreamName;
            this.statuses = statuses;
        }

        @SuppressWarnings("unused")
        public DescribeStreamResult describeStream(DescribeStreamRequest request)
        {
            int idx = describeInvocationCount.getAndIncrement();

            assertEquals("request contains stream name", expectedStreamName, request.getStreamName());

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
                // note: the base description leaves the shard list null, to fail tests that don't
                // configure needed shards

                StreamDescription description = new StreamDescription()
                        .withStreamName(request.getStreamName())
                        .withStreamStatus((StreamStatus)retval)
                        .withHasMoreShards(Boolean.FALSE);

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
            currentRetentionPeriod.set(request.getRetentionPeriodHours().intValue());
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
            currentRetentionPeriod.set(request.getRetentionPeriodHours().intValue());
        }
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testDescribeShardsSingleRetrieve() throws Exception
    {
        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.ACTIVE)
        {
            @Override
            protected void addToDescription(DescribeStreamRequest request, StreamDescription description)
            {
                description.withShards(SHARDS_1).withHasMoreShards(Boolean.FALSE);
            }
        };
        AmazonKinesis client = mock.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, STREAM_NAME, 1000);
        assertEquals("invocation count",        1, mock.describeInvocationCount.get());
        assertEquals("returned expected list",  SHARDS_1, shards);
    }


    @Test
    public void testDescribeShardsMultiRetrieve() throws Exception
    {
        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.ACTIVE)
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

        List<Shard> shards = KinesisUtils.describeShards(client, STREAM_NAME, 1000);
        assertEquals("invocation count",        2, mock.describeInvocationCount.get());
        assertEquals("returned expected list",  CollectionUtil.combine(new ArrayList<Shard>(), SHARDS_1, SHARDS_2), shards);
    }


    @Test
    public void testDescribeShardsStreamNotAvailable() throws Exception
    {
        AmazonKinesis client = new StreamDescriberMock(STREAM_NAME, ResourceNotFoundException.class)
                               .getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, STREAM_NAME, 1000);
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
        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.CREATING, StreamStatus.CREATING, StreamStatus.ACTIVE);
        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, STREAM_NAME, StreamStatus.ACTIVE, 500);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.ACTIVE, lastStatus);
        assertEquals("invocation count",    3, mock.describeInvocationCount.get());
        assertApproximate("elapsed time",   200, elapsed, 10);
    }


    @Test
    public void testWaitForStatusTimeout()
    {
        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.CREATING);
        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, STREAM_NAME, StreamStatus.ACTIVE, 250);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.CREATING, lastStatus);
        assertEquals("invocation count",    3, mock.describeInvocationCount.get());
        assertApproximate("elapsed time",   300, elapsed, 10);
    }


    @Test
    public void testWaitForStatusWithThrottling()
    {
        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME,
                                                           LimitExceededException.class, LimitExceededException.class,
                                                           StreamStatus.CREATING, StreamStatus.ACTIVE);
        AmazonKinesis client = mock.getInstance();

        // the expected sequence of calls:
        //  - limit exceeded, sleep for 100 ms
        //  - limit exceeded, sleep for 200 ms
        //  - return creating, sleep for 100 ms
        //  - return active, no sleep

        long start = System.currentTimeMillis();
        StreamStatus lastStatus = KinesisUtils.waitForStatus(client, STREAM_NAME, StreamStatus.ACTIVE, 500);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("status",              StreamStatus.ACTIVE, lastStatus);
        assertEquals("invocation count",    4, mock.describeInvocationCount.get());
        assertApproximate("elapsed time",   400, elapsed, 25);
    }


    @Test
    public void testCreateStreamHappyPath() throws Exception
    {
        final AtomicInteger createInvocationCount = new AtomicInteger(0);

        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, ResourceNotFoundException.class, StreamStatus.CREATING, StreamStatus.ACTIVE)
        {
            @SuppressWarnings("unused")
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                assertEquals("stream name passed to createStream", STREAM_NAME, request.getStreamName());
                assertEquals("shard count passed to createStream", 3,           request.getShardCount().intValue());
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
        StreamStatus status = KinesisUtils.createStream(client, STREAM_NAME, 3, 1000L);
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

        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.ACTIVE)
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
        StreamStatus status = KinesisUtils.createStream(client, STREAM_NAME, 3, 1000L);
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

        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.ACTIVE)
        {
            @SuppressWarnings("unused")
            public CreateStreamResult createStream(CreateStreamRequest request)
            {
                createInvocationCount.getAndIncrement();
                throw new ResourceInUseException("");
            }
        };
        AmazonKinesis client = mock.getInstance();

        StreamStatus status = KinesisUtils.createStream(client, STREAM_NAME, 3, 500L);

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
                assertEquals("stream name passed in request", STREAM_NAME, request.getStreamName());
                return new DeleteStreamResult();
            }
        }.getInstance();

        boolean status = KinesisUtils.deleteStream(client, STREAM_NAME, 500L);

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
        boolean status = KinesisUtils.deleteStream(client, STREAM_NAME, 250L);
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

        boolean status = KinesisUtils.deleteStream(client, STREAM_NAME, 250L);

        assertFalse("returned status indicates failure",        status);
        assertEquals("invocations of deleteStream",             1, invocationCount.get());
    }


    @Test
    public void testUpdateRetentionPeriodIncreaseHappyPath() throws Exception
    {
        // the expected sequence of calls:
        //  - wait for status
        //  - get descritpion
        //  - increase retention
        //  - updating, sleep for 100 ms
        //  - active

        RetentionPeriodMock mock = new RetentionPeriodMock(STREAM_NAME, 24, 36, StreamStatus.ACTIVE, StreamStatus.ACTIVE, StreamStatus.UPDATING, StreamStatus.ACTIVE)
        {
            @Override
            protected void decreaseStreamRetentionPeriodInternal(DecreaseStreamRetentionPeriodRequest request)
            {
                throw new UnsupportedOperationException("decreaseStreamRetentionPeriod should not be called by this test");
            }
        };

        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.updateRetentionPeriod(client, STREAM_NAME, 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.currentRetentionPeriod.get());
        assertEquals("invocations of describeStream",                   4,  mock.describeInvocationCount.get());
        assertEquals("invocations of increaseStreamRetentionPeriod",    1,  mock.increaseInvocationCount.get());
        assertEquals("invocations of decreaseStreamRetentionPeriod",    0,  mock.decreaseInvocationCount.get());
        assertApproximate("elapsed time",                               100, elapsed, 10);
    }


    @Test
    public void testUpdateRetentionPeriodIncreaseThrottling() throws Exception
    {
        // note: the Java SDK doesn't indicate that LimitExceededException is possible but the
        //       API docs do so we need to handle (and test for) it

        // the expected sequence of calls:
        //  - wait for status
        //  - initial describe
        //  - try to increase retention, throttled, sleep 100ms
        //  - wait for status
        //  - initial describe
        //  - try to increase retention, throttled, sleep 200ms
        //  - wait for status
        //  - initial describe
        //  - try to increase retention, success
        //  - active

        RetentionPeriodMock mock = new RetentionPeriodMock(STREAM_NAME, 24, 36, StreamStatus.ACTIVE)
        {
            @Override
            protected void increaseStreamRetentionPeriodInternal(IncreaseStreamRetentionPeriodRequest request)
            {
                // invocation count has already been incremented, so we throw twice not three times
                if (increaseInvocationCount.get() < 3)
                {
                    throw new LimitExceededException("");
                }
                super.increaseStreamRetentionPeriodInternal(request);
            }
        };

        AmazonKinesis client = mock.getInstance();
        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.updateRetentionPeriod(client, STREAM_NAME, 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.currentRetentionPeriod.get());
        assertEquals("invocations of describeStream",                   7,  mock.describeInvocationCount.get());
        assertEquals("invocations of increaseStreamRetentionPeriod",    3,  mock.increaseInvocationCount.get());
        assertApproximate("elapsed time",                               300, elapsed, 10);
    }


    @Test
    public void testUpdateRetentionPeriodIncreaseConcurrentUpdate() throws Exception
    {
        // we're going to try to increase the retention period but get blocked by a concurrent
        // call that increases it more than we planned, so we'll end up decreasing .. this is
        // an extremely convoluted example that's unlikely in real life, but verifies that the
        // logic is sound

        // the expected sequence of calls:
        //  - wait for status
        //  - initial describe
        //  - try to increase retention, throws ResourceInUseException, sleep for 100 ms
        //  - wait for status
        //  - initial describe
        //  - try to decrease retention, success
        //  - wait for status

        RetentionPeriodMock mock = new RetentionPeriodMock(STREAM_NAME, 24, 36, StreamStatus.ACTIVE)
        {
            @Override
            protected void increaseStreamRetentionPeriodInternal(IncreaseStreamRetentionPeriodRequest request)
            {
                // invocation count has already been incremented, so this happens on first call
                if (increaseInvocationCount.get() == 1)
                {
                    currentRetentionPeriod.set(48);
                    throw new ResourceInUseException("");
                }
                else
                {
                    // on the next call we should be trying to decrease the retention period
                    throw new IllegalStateException("shouldn't call increase twice");
                }
            }
        };

        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.updateRetentionPeriod(client, STREAM_NAME, 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.currentRetentionPeriod.get());
        assertEquals("invocations of describeStream",                   5,  mock.describeInvocationCount.get());
        assertEquals("invocations of increaseStreamRetentionPeriod",    1,  mock.increaseInvocationCount.get());
        assertEquals("invocations of decreaseStreamRetentionPeriod",    1,  mock.decreaseInvocationCount.get());
        assertApproximate("elapsed time",                               100, elapsed, 10);
    }


    @Test
    public void testUpdateRetentionPeriodIncreaseStreamDisappears() throws Exception
    {
        // this is even more unlikely than the previous test: the stream disappears
        // between an "ACTIVE" status and the increase/decrease call ... in fact, I
        // don't think it would ever happen, but as it's a documented exception I
        // need to support it

        // the expected sequence of calls:
        //  - wait for status
        //  - initial describe
        //  - try to increase retention, throws ResourceNotFoundException, stop trying

        RetentionPeriodMock mock = new RetentionPeriodMock(STREAM_NAME, 24, 36, StreamStatus.ACTIVE)
        {
            @Override
            protected void increaseStreamRetentionPeriodInternal(IncreaseStreamRetentionPeriodRequest request)
            {
                throw new ResourceNotFoundException("");
            }
        };

        AmazonKinesis client = mock.getInstance();

        StreamStatus status = KinesisUtils.updateRetentionPeriod(client, STREAM_NAME, 36, 1000L);

        assertEquals("final stream status",                             null, status);
        assertEquals("invocations of describeStream",                   2,  mock.describeInvocationCount.get());
        assertEquals("invocations of increaseStreamRetentionPeriod",    1,  mock.increaseInvocationCount.get());
        assertEquals("invocations of decreaseStreamRetentionPeriod",    0,  mock.decreaseInvocationCount.get());
    }


    @Test
    public void testUpdateRetentionPeriodDecreaseHappyPath() throws Exception
    {
        // sad path testing is handled by the various Increase tests; this just verifies
        // that we pick the correct call based on comparison of current and new period

        // the expected sequence of calls:
        //  - wait for status
        //  - get descritpion
        //  - increase retention
        //  - updating, sleep for 100 ms
        //  - active

        RetentionPeriodMock mock = new RetentionPeriodMock(STREAM_NAME, 48, 36, StreamStatus.ACTIVE, StreamStatus.ACTIVE, StreamStatus.UPDATING, StreamStatus.ACTIVE)
        {
            @Override
            protected void increaseStreamRetentionPeriodInternal(IncreaseStreamRetentionPeriodRequest request)
            {
                throw new UnsupportedOperationException("increaseStreamRetentionPeriod should not be called by this test");
            }
        };

        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.updateRetentionPeriod(client, STREAM_NAME, 36, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("final stream status",                             StreamStatus.ACTIVE, status);
        assertEquals("current retention period",                        36, mock.currentRetentionPeriod.get());
        assertEquals("invocations of describeStream",                   4,  mock.describeInvocationCount.get());
        assertEquals("invocations of increaseStreamRetentionPeriod",    0,  mock.increaseInvocationCount.get());
        assertEquals("invocations of decreaseStreamRetentionPeriod",    1,  mock.decreaseInvocationCount.get());
        assertApproximate("elapsed time",                               100, elapsed, 10);
    }


    @Test
    public void testReshardHappyPath() throws Exception
    {
        final AtomicInteger updateShardCountInvocationCount = new AtomicInteger(0);

        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.UPDATING, StreamStatus.ACTIVE)
        {
            private int activeShards = 1;
            private List<Shard> shards = Arrays.asList(
                new Shard().withShardId("shard-000")
                           .withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("00001")),
                new Shard().withShardId("shard-001")
                           .withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("00011")),
                new Shard().withShardId("shard-002")
                           .withSequenceNumberRange(new SequenceNumberRange().withStartingSequenceNumber("00011")));

            @SuppressWarnings("unused")
            public UpdateShardCountResult updateShardCount(UpdateShardCountRequest request)
            {
                updateShardCountInvocationCount.incrementAndGet();

                assertEquals("expected stream name", expectedStreamName, request.getStreamName());
                assertEquals("expected shard count", 2, request.getTargetShardCount().intValue());
                assertEquals("scaling type", ScalingType.UNIFORM_SCALING.toString(), request.getScalingType());

                activeShards = request.getTargetShardCount().intValue();
                if (activeShards > 0)
                {
                    shards.get(0).getSequenceNumberRange().withEndingSequenceNumber("00010");
                }
                // whitebox: we ignore the response so don't need to set anything here
                return new UpdateShardCountResult();
            }

            @Override
            protected void addToDescription(DescribeStreamRequest request, StreamDescription description)
            {
                if (activeShards == 1)
                    description.setShards(shards.subList(0, 1));
                else
                    description.setShards(shards);
            }
        };

        AmazonKinesis client = mock.getInstance();
        StreamStatus status = KinesisUtils.reshard(client, STREAM_NAME, 2, 1000);

        assertEquals("returned status",             StreamStatus.ACTIVE, status);
        assertEquals("update invocation count",     1, updateShardCountInvocationCount.get());
        // first describe gets UPDATING, second gets ACTIVE, third gets shards
        assertEquals("describe invocation count",   3, mock.describeInvocationCount.get());
    }


    @Test
    public void testReshardStatusTimout() throws Exception
    {
        final AtomicInteger updateShardCountInvocationCount = new AtomicInteger(0);

        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.UPDATING)
        {
            @SuppressWarnings("unused")
            public UpdateShardCountResult updateShardCount(UpdateShardCountRequest request)
            {
                updateShardCountInvocationCount.incrementAndGet();
                return new UpdateShardCountResult();
            }
        };

        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        StreamStatus status = KinesisUtils.reshard(client, STREAM_NAME, 2, 250);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("returned status",             null, status);
        assertEquals("update invocation count",     1, updateShardCountInvocationCount.get());
        // describe interaction
        // - first call
        // - sleep 100
        // - second call
        // - sleep 100
        // - third call
        // - sleep 100
        // - timeout
        assertEquals("describe invocation count",   3, mock.describeInvocationCount.get());
        NumericAsserts.assertInRange("elapsed time", 200, 350, elapsed);

        boolean wasWarningEmitted = false;
        for (LoggingEvent logEvent : loggingEvents)
        {
            if ((logEvent.getLevel() == Level.WARN)
                && logEvent.getRenderedMessage().contains("reshard")
                && logEvent.getRenderedMessage().contains("timed out")
                && logEvent.getRenderedMessage().contains(STREAM_NAME))
            {
                wasWarningEmitted = true;
            }
        }
        assertTrue("log included timeout warning", wasWarningEmitted);

    }


    @Test(expected=LimitExceededException.class)
    public void testReshardUnrecoverableException() throws Exception
    {
        StreamDescriberMock mock = new StreamDescriberMock(STREAM_NAME, StreamStatus.UPDATING)
        {
            @Override
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                fail("unexpected call to describeStream(): should have failed before now");
                return null;
            }

            @SuppressWarnings("unused")
            public UpdateShardCountResult updateShardCount(UpdateShardCountRequest request)
            {
                throw new LimitExceededException("example");
            }
        };

        AmazonKinesis client = mock.getInstance();
        KinesisUtils.reshard(client, "example", 2, 1000);
    }


    @Test
    public void testRetrieveShardIteratorLatest() throws Exception
    {
        // we're asserting the call so don't need to maintain a separate mock
        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                               request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.LATEST.toString(),        request.getShardIteratorType());
                assertEquals("sequence number",     null,                                       request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  null,                                       request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        }.getInstance();

        assertNotNull("return value", KinesisUtils.retrieveShardIterator(client, STREAM_NAME, "shard-0001", ShardIteratorType.LATEST, "123", NOW, 1000L));
    }


    @Test
    public void testRetrieveShardIteratorTrimHorizon() throws Exception
    {
        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                               request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.TRIM_HORIZON.toString(),  request.getShardIteratorType());
                assertEquals("sequence number",     null,                                       request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  null,                                       request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        }.getInstance();

        assertNotNull("return value", KinesisUtils.retrieveShardIterator(client, STREAM_NAME, "shard-0001", ShardIteratorType.TRIM_HORIZON, "123", NOW, 1000L));
    }


    @Test
    public void testRetrieveShardIteratorAtSequenceNumber() throws Exception
    {
        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                        request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                                       request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.AT_SEQUENCE_NUMBER.toString(),    request.getShardIteratorType());
                assertEquals("sequence number",     "123",                                              request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  null,                                               request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        }.getInstance();

        assertNotNull("return value", KinesisUtils.retrieveShardIterator(client, STREAM_NAME, "shard-0001", ShardIteratorType.AT_SEQUENCE_NUMBER, "123", NOW, 1000L));
    }


    @Test
    public void testRetrieveShardIteratorAfterSequenceNumber() throws Exception
    {
        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                        request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                                       request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                assertEquals("sequence number",     "123",                                              request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  null,                                               request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        }.getInstance();

        assertNotNull("return value", KinesisUtils.retrieveShardIterator(client, STREAM_NAME, "shard-0001", ShardIteratorType.AFTER_SEQUENCE_NUMBER, "123", NOW, 1000L));
    }


    @Test
    public void testRetrieveShardIteratorAtTimestamp() throws Exception
    {
        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                assertEquals("stream name",         STREAM_NAME,                                request.getStreamName());
                assertEquals("shard ID",            "shard-0001",                               request.getShardId());
                assertEquals("iterator type",       ShardIteratorType.AT_TIMESTAMP.toString(),  request.getShardIteratorType());
                assertEquals("sequence number",     null,                                       request.getStartingSequenceNumber());
                assertEquals("starting timestamp",  NOW,                                        request.getTimestamp());
                return new GetShardIteratorResult().withShardIterator("blah blah blah");
            }
        }.getInstance();

        assertNotNull("return value", KinesisUtils.retrieveShardIterator(client, STREAM_NAME, "shard-0001", ShardIteratorType.AT_TIMESTAMP, "123", NOW, 1000L));
    }


    @Test
    public void testRetrieveShardIteratorThrottling() throws Exception
    {
        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                throw new ProvisionedThroughputExceededException("");
            }
        }.getInstance();

        long start = System.currentTimeMillis();
        assertNull("return value", KinesisUtils.retrieveShardIterator(client, STREAM_NAME, "shard-0001", ShardIteratorType.AT_TIMESTAMP, "123", NOW, 250L));
        long elapsed = System.currentTimeMillis() - start;
        assertApproximate("elapsed time", 300, elapsed, 10);
    }


    @Test
    public void testMapConversions() throws Exception
    {
        Shard shard1 = new Shard().withShardId("shard-0001");
        Shard shard2 = new Shard().withShardId("shard-0002");
        Shard shard3 = new Shard().withShardId("shard-0003").withParentShardId("shard-0001");
        Shard shard4 = new Shard().withShardId("shard-0004").withParentShardId("shard-0001");
        List<Shard> allShards = Arrays.asList(shard1, shard2, shard3, shard4);

        Map<String,Shard> shardsById = KinesisUtils.toMapById(allShards);
        assertEquals("shard map size", 4, shardsById.size());
        for (Shard shard : allShards)
        {
            assertSame("shard map contains " + shard.getShardId(), shard, shardsById.get(shard.getShardId()));
        }

        Map<String,List<Shard>> shardsByParent = KinesisUtils.toMapByParentId(allShards);
        assertEquals("parent map size", 2, shardsByParent.size());
        assertEquals("ultimate parents",  Arrays.asList(shard1, shard2), shardsByParent.get(null));
        assertEquals("expected children", Arrays.asList(shard3, shard4), shardsByParent.get(shard1.getShardId()));
    }

}
