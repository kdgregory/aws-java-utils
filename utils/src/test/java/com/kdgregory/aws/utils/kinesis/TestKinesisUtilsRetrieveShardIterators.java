// Copyright (c) Keith D Gregory, all rights reserved
package com.kdgregory.aws.utils.kinesis;

import java.util.Arrays;
import java.util.HashMap;
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
public class TestKinesisUtilsRetrieveShardIterators
{
//----------------------------------------------------------------------------
//  Some preconstructed sample shards -- all will have a name and starting
//  sequence number; test is responsible for doing anything else
//----------------------------------------------------------------------------

    private final static String SHARD001_ID     = "shard-001";
    private final static String SHARD001_START  = "000000";
    private final static String SHARD001_MID    = "059999";
    private final static String SHARD001_END    = "099999";

    private final static String SHARD002_ID     = "shard-002";
    private final static String SHARD002_START  = "100000";
    private final static String SHARD002_MID    = "159999";
    private final static String SHARD002_END    = "199999";

    private final static String SHARD003_ID     = "shard-003";
    private final static String SHARD003_START  = "200000";
    private final static String SHARD003_MID    = "259999";
    private final static String SHARD003_END    = "299999";

    private Shard shard001 = new Shard().withShardId(SHARD001_ID)
                                        .withSequenceNumberRange
                                            (new SequenceNumberRange().withStartingSequenceNumber(SHARD001_START));
    private Shard shard002 = new Shard().withShardId(SHARD002_ID)
                                        .withSequenceNumberRange
                                            (new SequenceNumberRange().withStartingSequenceNumber(SHARD002_START));
    private Shard shard003 = new Shard().withShardId(SHARD003_ID)
                                        .withSequenceNumberRange
                                            (new SequenceNumberRange().withStartingSequenceNumber(SHARD003_START));

//----------------------------------------------------------------------------
//  Support Code
//----------------------------------------------------------------------------

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
//  Tests
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
                assertEquals("shard ID",        SHARD001_ID,                                request.getShardId());
                assertEquals("iterator type",   ShardIteratorType.TRIM_HORIZON.toString(),  request.getShardIteratorType());
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.TRIM_HORIZON, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNotEmpty("returned iterator for shard",       iterators.get(SHARD001_ID));
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
        assertNotEmpty("returned iterator for shard 1",     iterators.get(SHARD001_ID));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD002_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  2, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testMultipleShardsWithStoredOffsetsSimple() throws Exception
    {
        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARD001_ID, SHARD001_MID);
        offsets.put(SHARD002_ID, SHARD002_MID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                assertEquals("shard iterator type", ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                if (request.getShardId().equals(SHARD001_ID))
                {
                    assertEquals("shard 001 sequence number", SHARD001_MID, request.getStartingSequenceNumber());
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    assertEquals("shard 002 sequence number", SHARD002_MID, request.getStartingSequenceNumber());
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", offsets, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                2, iterators.size());
        assertNotEmpty("returned iterator for shard 1",     iterators.get(SHARD001_ID));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD002_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  2, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testMultipleShardsWithSomeStoredOffsets() throws Exception
    {
        // this is an unlikely real-world case unless the caller makes a mistake

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARD001_ID, SHARD001_MID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    assertEquals("shard 001 iterator type",     ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                    assertEquals("shard 001 sequence number",   SHARD001_MID,                                       request.getStartingSequenceNumber());
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    assertEquals("shard 002 iterator type",     ShardIteratorType.LATEST.toString(),    request.getShardIteratorType());
                    assertEquals("shard 002 sequence number",   null,                                   request.getStartingSequenceNumber());
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", offsets, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                2, iterators.size());
        assertNotEmpty("returned iterator for shard 1",     iterators.get(SHARD001_ID));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD002_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  2, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testClosedShardWithStoredOffsets() throws Exception
    {
        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARD001_ID, SHARD001_MID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                assertEquals("shard 001 iterator type",     ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                assertEquals("shard 001 sequence number",   SHARD001_MID,                                       request.getStartingSequenceNumber());
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", offsets, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNotEmpty("returned iterator for shard 1",     iterators.get(SHARD001_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testPickParentIfNoOffsetsAndTrimHorizon() throws Exception
    {
        shard001.getSequenceNumberRange().setEndingSequenceNumber(SHARD001_END);
        shard002.setParentShardId(SHARD001_ID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    assertEquals("shard 001 iterator type", ShardIteratorType.TRIM_HORIZON.toString(), request.getShardIteratorType());
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    fail("shard 002 should not have been queried");
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.TRIM_HORIZON, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNotEmpty("returned iterator for shard 1",     iterators.get(SHARD001_ID));
        assertNull("did not return iterator for shard 2",   iterators.get(SHARD002_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testPickChildIfNoOffsetsAndLatest() throws Exception
    {
        shard001.getSequenceNumberRange().setEndingSequenceNumber(SHARD001_END);
        shard002.setParentShardId(SHARD001_ID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    fail("shard 001 should not have been queried");
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    assertEquals("shard 002 iterator type", ShardIteratorType.LATEST.toString(), request.getShardIteratorType());
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNull("did not return iterator for shard 1",   iterators.get(SHARD001_ID));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD002_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testPickParentWhenRelevant() throws Exception
    {
        shard001.getSequenceNumberRange().setEndingSequenceNumber(SHARD001_END);
        shard002.setParentShardId(SHARD001_ID);

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARD001_ID, SHARD001_MID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    assertEquals("shard 001 iterator type",     ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                    assertEquals("shard 001 sequence number",   SHARD001_MID,                                       request.getStartingSequenceNumber());
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    fail("shard 002 should not have been queried");
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", offsets, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNotEmpty("returned iterator for shard 1",     iterators.get(SHARD001_ID));
        assertNull("did not returned iterator for shard 2", iterators.get(SHARD002_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testPickChildWhenParentNotRelevant() throws Exception
    {
        shard001.getSequenceNumberRange().setEndingSequenceNumber(SHARD001_END);
        shard002.setParentShardId(SHARD001_ID);

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARD002_ID, SHARD002_MID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    fail("shard 001 should not have been queried");
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    assertEquals("shard 002 iterator type",     ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                    assertEquals("shard 002 sequence number",   SHARD002_MID,                                       request.getStartingSequenceNumber());
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", offsets, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNull("did not returned iterator for shard 1", iterators.get(SHARD001_ID));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD002_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testParentClosedOneChildHasOffsetsOneDoesnt() throws Exception
    {
        shard001.getSequenceNumberRange().setEndingSequenceNumber(SHARD001_END);
        shard002.setParentShardId(SHARD001_ID);
        shard003.setParentShardId(SHARD001_ID);

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARD002_ID, SHARD002_MID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002, shard003)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    fail("shard 001 should not have been queried");
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    assertEquals("shard 002 iterator type",     ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                    assertEquals("shard 002 sequence number",   SHARD002_MID,                                       request.getStartingSequenceNumber());
                }
                if (request.getShardId().equals(SHARD003_ID))
                {
                    assertEquals("shard 003 iterator type",     ShardIteratorType.TRIM_HORIZON.toString(),          request.getShardIteratorType());
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", offsets, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                2, iterators.size());
        assertNull("did not returned iterator for shard 1", iterators.get(SHARD001_ID));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD002_ID));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD003_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  2, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testGrandparentAndParentClosedNoOffsetsIteratorTypeLatest() throws Exception
    {
        shard001.getSequenceNumberRange().setEndingSequenceNumber(SHARD001_END);
        shard002.setParentShardId(SHARD001_ID);
        shard002.getSequenceNumberRange().setEndingSequenceNumber(SHARD002_END);
        shard003.setParentShardId(SHARD002_ID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002, shard003)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    fail("shard 001 should not have been queried");
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    fail("shard 002 should not have been queried");
                }
                if (request.getShardId().equals(SHARD003_ID))
                {
                    assertEquals("shard 003 iterator type", ShardIteratorType.LATEST.toString(), request.getShardIteratorType());
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.LATEST, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNull("did not returned iterator for shard 1", iterators.get(SHARD001_ID));
        assertNull("did not returned iterator for shard 2", iterators.get(SHARD002_ID));
        assertNotEmpty("returned iterator for shard 3",     iterators.get(SHARD003_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testGrandparentAndParentClosedNoOffsetsIteratorTypeTrimHorizon() throws Exception
    {
        shard001.getSequenceNumberRange().setEndingSequenceNumber(SHARD001_END);
        shard002.setParentShardId(SHARD001_ID);
        shard002.getSequenceNumberRange().setEndingSequenceNumber(SHARD002_END);
        shard003.setParentShardId(SHARD002_ID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002, shard003)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    assertEquals("shard 001 iterator type", ShardIteratorType.TRIM_HORIZON.toString(), request.getShardIteratorType());
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    fail("shard 002 should not have been queried");
                }
                if (request.getShardId().equals(SHARD003_ID))
                {
                    fail("shard 003 should not have been queried");
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.TRIM_HORIZON, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNull("did not returned iterator for shard 1", iterators.get(SHARD001_ID));
        assertNull("did not returned iterator for shard 2", iterators.get(SHARD002_ID));
        assertNotEmpty("returned iterator for shard 3",     iterators.get(SHARD003_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
    }


    @Test
    public void testGrandparentAndParentClosedOffsetsPointToParent() throws Exception
    {
        shard001.getSequenceNumberRange().setEndingSequenceNumber(SHARD001_END);
        shard002.setParentShardId(SHARD001_ID);
        shard002.getSequenceNumberRange().setEndingSequenceNumber(SHARD002_END);
        shard003.setParentShardId(SHARD002_ID);

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARD002_ID, SHARD002_MID);

        KinesisClientMock mock = new KinesisClientMock("example", shard001, shard002, shard003)
        {
            @Override
            protected String getShardIterator0(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARD001_ID))
                {
                    fail("shard 001 should not have been queried");
                }
                if (request.getShardId().equals(SHARD002_ID))
                {
                    assertEquals("shard 002 iterator type",     ShardIteratorType.AFTER_SEQUENCE_NUMBER.toString(), request.getShardIteratorType());
                    assertEquals("shard 002 sequence number",   SHARD002_MID,                                       request.getStartingSequenceNumber());
                }
                if (request.getShardId().equals(SHARD003_ID))
                {
                    fail("shard 003 should not have been queried");
                }
                return super.getShardIterator0(request);
            }
        };
        AmazonKinesis client = mock.getInstance();

        Map<String,String> iterators = KinesisUtils.retrieveShardIterators(client, "example", null, ShardIteratorType.TRIM_HORIZON, 1000L);

        assertEquals("size of returned map",                1, iterators.size());
        assertNull("did not returned iterator for shard 1", iterators.get(SHARD001_ID));
        assertNotEmpty("returned iterator for shard 2",     iterators.get(SHARD002_ID));
        assertNull("did not returned iterator for shard 3", iterators.get(SHARD003_ID));
        assertEquals("invocation count, describeStream",    1, mock.describeInvocationCount.get());
        assertEquals("invocation count, getShardIterator",  1, mock.retrieveInvocationCount.get());
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
        assertNotEmpty("returned iterator for shard",       iterators.get(SHARD001_ID));
        assertNotEmpty("returned iterator for shard",       iterators.get(SHARD002_ID));
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
