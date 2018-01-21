// Copyright (c) Keith D Gregory, all rights reserved
package com.kdgregory.aws.utils.kinesis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import static org.junit.Assert.*;

import net.sf.kdgcommons.test.SelfMock;
import static net.sf.kdgcommons.test.NumericAsserts.*;
import static net.sf.kdgcommons.test.StringAsserts.*;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


// there are enough combinations that we need to test that this deserves its own class
public class TestKinesisUtilsRetreiveShardIterators
{
//----------------------------------------------------------------------------
//  Support Code
//----------------------------------------------------------------------------

    // standard shard names -- these correspond to shards with lowercased name
    private final static String SHARD001 = "shard-001";
    private final static String SHARD002 = "shard-002";

    // some shards to use as a base for building more complex behaviors
    // (note that these are instance variables so will start each test fresh)
    private Shard shard001 = new Shard().withShardId(SHARD001);
    private Shard shard002 = new Shard().withShardId(SHARD002);


    /**
     *  Mocks out the calls to describeStream() and getShardIterator(). Tests
     *  will override {@link #getShardIterator0} to perform assertions on the
     *  passed request and potentially return a specific iterator value.
     */
    private static class KinesisClientMock extends SelfMock<AmazonKinesis>
    {
        public AtomicInteger describeInvocationCount = new AtomicInteger();
        public AtomicInteger retrieveInvocationCount = new AtomicInteger();

        protected String expectedStreamName;
        protected List<Shard> shards;

        public KinesisClientMock(String expectedStreamName, Shard... shards)
        {
            super(AmazonKinesis.class);
            this.expectedStreamName = expectedStreamName;
            this.shards = Arrays.asList(shards);
        }

        @SuppressWarnings("unused")
        public DescribeStreamResult describeStream(DescribeStreamRequest request)
        {
            describeInvocationCount.getAndIncrement();
            assertEquals("request contains stream name", expectedStreamName, request.getStreamName());
            return new DescribeStreamResult()
                        .withStreamDescription(new StreamDescription()
                            .withStreamName(expectedStreamName)
                            .withShards(shards)
                            .withHasMoreShards(Boolean.FALSE));
        }

        @SuppressWarnings("unused")
        public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
        {
            retrieveInvocationCount.getAndIncrement();
            return new GetShardIteratorResult().withShardIterator(getShardIterator0(request));
        }


        protected String getShardIterator0(GetShardIteratorRequest request)
        {
            return UUID.randomUUID().toString();
        }
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testSingleShardNoStoredOffsets() throws Exception
    {
        KinesisClientMock mock = new KinesisClientMock("example", shard001)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                assertEquals("stream name",     expectedStreamName,                         request.getStreamName());
                assertEquals("shard ID",        SHARD001,                                   request.getShardId());
                assertEquals("iterator type",   ShardIteratorType.TRIM_HORIZON.toString(),  request.getShardIteratorType());
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.TRIM_HORIZON, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNotEmpty("returned iterator for shard",       iterators.get(SHARD001));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testMultipleShardsNoStoredOffsets() throws Exception
    {
        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                assertEquals("stream name",     expectedStreamName,                  request.getStreamName());
                assertEquals("iterator type",   ShardIteratorType.LATEST.toString(), request.getShardIteratorType());
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                2, iterators.size());
        assertNotEmpty("returned iterator for shard 1",     iterators.get(SHARD001));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD002));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  2, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testThrottling() throws Exception
    {
        // expected call sequence:
        // - describe
        // - retrieve, throttled, wait 100ms
        // - retrieve, successful
        // - retrieve, throttled, wait 100ms
        // - retrieve, successful

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                // note: invocation count is pre-incremented, so this will be 1st and 3rd calls
                if ((retrieveInvocationCount.get() % 2) == 1)
                    throw new ProvisionedThroughputExceededException("");
                else
                    return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.TRIM_HORIZON, 1000L);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("size of returned map",                2, iterators.size());
        assertNotEmpty("returned iterator for shard",       iterators.get(SHARD001));
        assertNotEmpty("returned iterator for shard",       iterators.get(SHARD002));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  4, mock.retrieveInvocationCount.get());
        assertApproximate("total execution time",           200, elapsed, 10);
    }


    @Test
    public void testTimeoutInDescribe() throws Exception
    {
        // expected call sequence:
        // - describe, throttled, sleep 100ms
        // - describe, throttled, sleep 200ms

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                throw new LimitExceededException("");
            }

            @Override
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                fail("should never get to this point");
                return null;
            }

        };
        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.TRIM_HORIZON, 250L);
        long elapsed = System.currentTimeMillis() - start;

        assertNull("did not return a result",               iterators);
        assertApproximate("total execution time",           300, elapsed, 10);
    }


    @Test
    public void testTimeoutWhenRetrievingIterators() throws Exception
    {
        // expected call sequence:
        // - describe
        // - retrieve, throttled, wait 100ms
        // - retrieve, throttled, wait 200ms

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                throw new ProvisionedThroughputExceededException("");
            }
        };
        AmazonKinesis client = mock.getInstance();

        long start = System.currentTimeMillis();
        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.TRIM_HORIZON, 250L);
        long elapsed = System.currentTimeMillis() - start;

        assertNull("did not return a result",               iterators);
        assertApproximate("total execution time",           300, elapsed, 10);
    }
}
