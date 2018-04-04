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

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.*;
import org.apache.log4j.spi.*;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.lang.StringUtil;
import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.util.BinaryUtils;


/**
 *  Mock-object tests of KinesisUtils.
 */
public class TestKinesisReader
{
    private Logger myLogger = Logger.getLogger(getClass());

    private LinkedBlockingQueue<LoggingEvent> loggingEvents = new LinkedBlockingQueue<LoggingEvent>();

//----------------------------------------------------------------------------
//  Some common test parameters
//----------------------------------------------------------------------------

    private final static String STREAM_NAME = "example";

    private final static String SHARDID_0 = formatShardId(0);
    private final static String SHARDID_1 = formatShardId(1);
    private final static String SHARDID_2 = formatShardId(2);
    private final static String SHARDID_3 = formatShardId(3);
    private final static String SHARDID_4 = formatShardId(4);

    private final static List<String> RECORDS_0 = Arrays.asList("foo", "bar", "baz");
    private final static List<String> RECORDS_1 = Arrays.asList("argle", "bargle");
    private final static List<String> RECORDS_2 = Arrays.asList("bingo", "zippy");
    private final static List<String> RECORDS_3 = Arrays.asList("crunchy", "bits");
    private final static List<String> RECORDS_4 = Arrays.asList("norwegian", "blue");


//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    private static String formatShardId(int id)
    {
        return String.format("shard-%03d", id);
    }


    private static String formatSequenceNumber(String shardId, int index)
    {
        return shardId + "," + String.format("%06d", index);
    }


    private static String extractShardIdFromSeqnum(String sequenceNumber)
    {
        return sequenceNumber.split(",")[0];
    }


    private static int extractOffsetFromSeqnum(String sequenceNumber)
    {
        return Integer.parseInt(sequenceNumber.split(",")[1]);
    }


    /**
     *  Identifies all log messages that contain the specified text, and asserts
     *  their level.
     */
    public void assertLogMessages(String message, String searchtext, int expectedCount, Level expectedLevel)
    {
        int actualCount = 0;
        for (LoggingEvent logEvent : loggingEvents)
        {
            if (logEvent.getRenderedMessage().contains(searchtext))
            {
                assertEquals(message + ": log level", expectedLevel, logEvent.getLevel());
                actualCount++;
            }
        }
        assertEquals(message + ": number of messages", expectedCount, actualCount);
    }


    /**
     *  Retrieves all records from a single iteration of the reader. Verifies
     *  that the reader's offsets are updated after each record is read.
     */
    private static List<String> retrieveRecords(KinesisReader reader)
    throws Exception
    {
        List<String> retrievedRecords = new ArrayList<String>();
        for (Record record : reader)
        {
            byte[] recordData = BinaryUtils.copyAllBytesFrom(record.getData());
            retrievedRecords.add(new String(recordData, "UTF-8"));
            assertEquals("offsets have been updated",
                         record.getSequenceNumber(),
                         reader.getCurrentSequenceNumbers().get(extractShardIdFromSeqnum(record.getSequenceNumber())));
        }
        return retrievedRecords;
    }


    /**
     *  Uses reflection to extract the current shard iterators from the reader.
     */
    private Map<String,String> getShardIterators(KinesisReader reader)
    throws Exception
    {
        Field fShardManager = reader.getClass().getDeclaredField("shardManager");
        fShardManager.setAccessible(true);
        Object shardManager = fShardManager.get(reader);

        Field fShardIterators = shardManager.getClass().getDeclaredField("shardIterators");
        fShardIterators.setAccessible(true);
        return (Map<String,String>)fShardIterators.get(shardManager);
    }


    /**
     *  A mock class that supports just enough behavior to make the reader work.
     *  Construct with one or more lists of strings that represent the records
     *  in a shard.
     *  <p>
     *  Note: to simplify mock implementation, shard iterators and sequence
     *  numbers use the same format.
     */
    public static class KinesisMock extends SelfMock<AmazonKinesis>
    {
        private String expectedStreamName;
        private Map<String,List<Record>> recordsByShard = new HashMap<String,List<Record>>();

        public KinesisMock(String expectedStreamName, List<String>... contentToReturn)
        {
            super(AmazonKinesis.class);
            this.expectedStreamName = expectedStreamName;
            for (int ii = 0 ; ii < contentToReturn.length ; ii++)
            {
                String shardId = formatShardId(ii);
                List<Record> records = new ArrayList<Record>();
                for (int jj = 0 ; jj < contentToReturn[ii].size() ; jj++)
                {
                    byte[] data = StringUtil.toUTF8(contentToReturn[ii].get(jj));
                    records.add(new Record()
                                .withSequenceNumber(formatSequenceNumber(shardId, jj))
                                .withPartitionKey("ignored")
                                .withData(ByteBuffer.wrap(data)));
                }
                recordsByShard.put(shardId, records);
            }
        }

        public DescribeStreamResult describeStream(DescribeStreamRequest request)
        {
            assertEquals("request contains stream name", expectedStreamName, request.getStreamName());

            return new DescribeStreamResult()
                       .withStreamDescription(new StreamDescription()
                           .withStreamName(expectedStreamName)
                           .withStreamStatus(StreamStatus.ACTIVE)
                           .withShards(createShards(recordsByShard.keySet()))
                           .withHasMoreShards(Boolean.FALSE));
        }

        public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
        {
            assertShardIteratorRequest(request);

            String shardId = request.getShardId();

            int offset = 0;
            switch (ShardIteratorType.fromValue(request.getShardIteratorType()))
            {
                case TRIM_HORIZON :
                    // default value is OK
                    break;
                case LATEST :
                    offset = recordsByShard.get(shardId).size();
                    break;
                case AFTER_SEQUENCE_NUMBER :
                    offset = extractOffsetFromSeqnum(request.getStartingSequenceNumber()) + 1;
                    break;
                default :
                    throw new IllegalArgumentException("unexpected shard iterator type: " + request.getShardIteratorType());
            }

            return new GetShardIteratorResult().withShardIterator(formatSequenceNumber(shardId, offset));
        }

        public GetRecordsResult getRecords(GetRecordsRequest request)
        {
            String shardItx = request.getShardIterator();
            String shardId = extractShardIdFromSeqnum(shardItx);
            int offset = extractOffsetFromSeqnum(shardItx);
            List<Record> shardRecords = recordsByShard.get(shardId);
            List<Record> remainingRecords = shardRecords.subList(offset, shardRecords.size());
            List<Record> returnedRecords = limitReturnedRecords(remainingRecords);
            String nextShardItx = (offset + returnedRecords.size() == shardRecords.size())
                                ? null
                                : formatSequenceNumber(shardId, offset + returnedRecords.size());

            return new GetRecordsResult()
                   .withRecords(returnedRecords)
                   .withNextShardIterator(nextShardItx)
                   .withMillisBehindLatest(Long.valueOf(0));
        }

        // hooks for subclasses

        protected List<Shard> createShards(Collection<String> shardIds)
        {
            List<Shard> shards = new ArrayList<Shard>();
            for (String shardId : shardIds)
            {
                shards.add(new Shard()
                           .withShardId(shardId)
                           .withSequenceNumberRange(new SequenceNumberRange()
                                                    .withStartingSequenceNumber(formatSequenceNumber(shardId, 0))));
            }
            return shards;
        }

        protected void assertShardIteratorRequest(GetShardIteratorRequest request)
        {
            // default does nothing
        }

        protected List<Record> limitReturnedRecords(List<Record> records)
        {
            return records;
        }
    }

//----------------------------------------------------------------------------
//  Setup and teardown
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        Logger readerLogger = Logger.getLogger(KinesisReader.class);
        readerLogger.setLevel(Level.DEBUG);
        readerLogger.addAppender(new AppenderSkeleton()
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
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testSingleShardNoOffsetsDefaultTrimHorizon() throws Exception
    {
        myLogger.info("testSingleShardNoOffsetsDefaultTrimHorizon");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0)
        {
            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                assertEquals("iterator type", ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        assertEquals("retrieved records", RECORDS_0, retrieveRecords(reader));
        assertNull("at end of stream, iterator is null", getShardIterators(reader).get(SHARDID_0));
    }


    @Test
    public void testSingleShardNoOffsetsDefaultLatest() throws Exception
    {
        myLogger.info("testSingleShardNoOffsetsDefaultLatest");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0)
        {
            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                assertEquals("iterator type", ShardIteratorType.LATEST, ShardIteratorType.fromValue(request.getShardIteratorType()));
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME);

        assertEquals("retrieved records", Collections.emptyList(), retrieveRecords(reader));
        assertNull("at end of stream, iterator is null", getShardIterators(reader).get(SHARDID_0));
    }


    @Test
    public void testSingleShardWithOffsets() throws Exception
    {
        myLogger.info("testSingleShardWithOffsets");

        final String sequenceNumber0 = formatSequenceNumber(SHARDID_0, 0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0)
        {
            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                assertEquals("iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                assertEquals("sequence number", sequenceNumber0,                         request.getStartingSequenceNumber());
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARDID_0, sequenceNumber0);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("retrieved records",
                     RECORDS_0.subList(1, RECORDS_0.size()),
                     retrieveRecords(reader));

        assertNull("at end of stream, iterator is null", getShardIterators(reader).get(SHARDID_0));
    }


    @Test
    public void testSingleShardRepeatedRead() throws Exception
    {
        myLogger.info("testSingleShardRepeatedRead");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0)
        {
            @Override
            protected List<Record> limitReturnedRecords(List<Record> records)
            {
                if (records.size() > 2)
                    records = records.subList(0, 2);
                return records;
            }

        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        assertEquals("first read",
                     RECORDS_0.subList(0, 2),
                     retrieveRecords(reader));
        assertEquals("after first read, iterator has been updated",
                     formatSequenceNumber(SHARDID_0, 2),
                     getShardIterators(reader).get(SHARDID_0));

        assertEquals("second read",
                     RECORDS_0.subList(2, RECORDS_0.size()),
                     retrieveRecords(reader));
        assertNull("at end of stream, iterator is null", getShardIterators(reader).get(SHARDID_0));
    }


    @Test
    public void testSingleShardAtEndOfStreamDoesNotRepeat() throws Exception
    {
        myLogger.info("testSingleShardAtEndOfStreamDoesNotRepeat");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        assertEquals("records from first read",     RECORDS_0,               retrieveRecords(reader));
        assertEquals("records from second read",    Collections.emptyList(), retrieveRecords(reader));
    }


    @Test
    public void testMultipleShardsNoOffsetsDefaultTrimHorizon() throws Exception
    {
        myLogger.info("testMultipleShardsNoOffsetsDefaultTrimHorizon");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1);
        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        assertEquals("retrieved records",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_0, RECORDS_1),
                     retrieveRecords(reader));

        assertNull("at end of streams, shard 0 iterator is null", getShardIterators(reader).get(SHARDID_0));
        assertNull("at end of streams, shard 1 iterator is null", getShardIterators(reader).get(SHARDID_1));
    }


    @Test
    public void testMultipleShardsWithOffsets() throws Exception
    {
        myLogger.info("testMultipleShardsWithOffsets");

        final String sequenceNumber0 = formatSequenceNumber(SHARDID_0, 0);
        final String sequenceNumber1 = formatSequenceNumber(SHARDID_1, 0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1)
        {
            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARDID_0))
                {
                    assertEquals("shard 0 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 0 sequence number", sequenceNumber0, request.getStartingSequenceNumber());
                }
                else if (request.getShardId().equals(SHARDID_1))
                {
                    assertEquals("shard 1 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 1 sequence number", sequenceNumber1, request.getStartingSequenceNumber());
                }
                else
                {
                    fail("unexpected shard: " + request.getShardId());
                }
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARDID_0, sequenceNumber0);
        offsets.put(SHARDID_1, sequenceNumber1);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("retrieved records",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_0.subList(1, RECORDS_0.size()),
                                                                     RECORDS_1.subList(1, RECORDS_1.size())),
                     retrieveRecords(reader));
        assertNull("at end of streams, shard 0 iterator is null", getShardIterators(reader).get(SHARDID_0));
        assertNull("at end of streams, shard 1 iterator is null", getShardIterators(reader).get(SHARDID_1));
    }


    @Test
    public void testMultipleShardsOnlyOneHasOffsets() throws Exception
    {
        myLogger.info("testMultipleShardsOnlyOneHasOffsets");

        final String sequenceNumber0 = formatSequenceNumber(SHARDID_0, 0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1)
        {
            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARDID_0))
                {
                    assertEquals("shard 0 iterator type", ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 0 sequence number", sequenceNumber0, request.getStartingSequenceNumber());
                }
                else if (request.getShardId().equals(SHARDID_1))
                {
                    assertEquals("shard 1 iterator type", ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                }
                else
                {
                    fail("unexpected shard: " + request.getShardId());
                }
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARDID_0, sequenceNumber0);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("retrieved records",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_0.subList(1, RECORDS_0.size()),
                                                                     RECORDS_1),
                     retrieveRecords(reader));

        assertNull("at end of streams, shard 0 iterator is null", getShardIterators(reader).get(SHARDID_0));
        assertNull("at end of streams, shard 1 iterator is null", getShardIterators(reader).get(SHARDID_1));
    }


    @Test
    public void testParentIsConsumedBeforeChild() throws Exception
    {
        myLogger.info("testParentIsConsumedBeforeChild");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1)
        {
            @Override
            protected List<Shard> createShards(Collection<String> shardIds)
            {
                Shard shard0 = new Shard()
                                 .withShardId(SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_0, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(SHARDID_0, RECORDS_0.size())));

                Shard shard1 = new Shard()
                                 .withShardId(SHARDID_1)
                                 .withParentShardId(SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_1, 0)));

                return Arrays.asList(shard0, shard1);
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        assertEquals("records from first read", RECORDS_0, retrieveRecords(reader));
        assertNotNull("after first read, has offsets for parent",   reader.getCurrentSequenceNumbers().get(SHARDID_0));
        assertNull("after first read, has no offsets for child",     reader.getCurrentSequenceNumbers().get(SHARDID_1));

        assertEquals("records from second read", RECORDS_1, retrieveRecords(reader));
        assertNull("after second read, has no offsets for parent",  reader.getCurrentSequenceNumbers().get(SHARDID_0));
        assertNotNull("after second read, has offsets for child",   reader.getCurrentSequenceNumbers().get(SHARDID_1));
    }


    @Test
    public void testUnbalancedTreeOffsetsOnShortSide() throws Exception
    {
        myLogger.info("testUnbalancedTreeOffsetsOnShortSide");

        final String sequenceNumber1 = formatSequenceNumber(SHARDID_1, 0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1, RECORDS_2, RECORDS_3, RECORDS_4)
        {
            @Override
            protected List<Shard> createShards(Collection<String> shardIds)
            {
                Shard shard0 = new Shard()
                                 .withShardId(SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_0, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(SHARDID_0, RECORDS_0.size())));

                Shard shard1 = new Shard()
                                 .withShardId(SHARDID_1)
                                 .withParentShardId(SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_1, 0)));

                Shard shard2 = new Shard()
                                 .withShardId(SHARDID_2)
                                 .withParentShardId(SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_2, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(SHARDID_2, RECORDS_2.size())));

                Shard shard3 = new Shard()
                                 .withShardId(SHARDID_3)
                                 .withParentShardId(SHARDID_2)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_3, 0)));

                Shard shard4 = new Shard()
                                 .withShardId(SHARDID_4)
                                 .withParentShardId(SHARDID_2)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_4, 0)));

                return Arrays.asList(shard0, shard1, shard2, shard3, shard4);
            }

            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARDID_0))
                {
                    fail("should not have queried shard 0");
                }
                else if (request.getShardId().equals(SHARDID_1))
                {
                    assertEquals("shard 1 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 1 sequence number", sequenceNumber1,                         request.getStartingSequenceNumber());
                }
                else
                {
                    assertEquals(request.getShardId() + " iterator type", ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                }
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARDID_1, sequenceNumber1);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("records from first read",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_1.subList(1, RECORDS_1.size()),
                                                                     RECORDS_2),
                     retrieveRecords(reader));

        assertEquals("records from second read",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_3, RECORDS_4),
                     retrieveRecords(reader));

        assertEquals("records from third read",
                     Collections.emptyList(),
                     retrieveRecords(reader));
    }


    @Test
    public void testUnbalancedTreeOffsetsOnLongSide() throws Exception
    {
        myLogger.info("testUnbalancedTreeOffsetsOnLongSide");

        final String sequenceNumber4 = formatSequenceNumber(SHARDID_4, 0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1, RECORDS_2, RECORDS_3, RECORDS_4)
        {
            @Override
            protected List<Shard> createShards(Collection<String> shardIds)
            {
                Shard shard0 = new Shard()
                                 .withShardId(SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_0, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(SHARDID_0, RECORDS_0.size())));

                Shard shard1 = new Shard()
                                 .withShardId(SHARDID_1)
                                 .withParentShardId(SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_1, 0)));

                Shard shard2 = new Shard()
                                 .withShardId(SHARDID_2)
                                 .withParentShardId(SHARDID_0)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_2, 0))
                                     .withEndingSequenceNumber(formatSequenceNumber(SHARDID_2, RECORDS_2.size())));

                Shard shard3 = new Shard()
                                 .withShardId(SHARDID_3)
                                 .withParentShardId(SHARDID_2)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_3, 0)));

                Shard shard4 = new Shard()
                                 .withShardId(SHARDID_4)
                                 .withParentShardId(SHARDID_2)
                                 .withSequenceNumberRange(
                                     new SequenceNumberRange()
                                     .withStartingSequenceNumber(formatSequenceNumber(SHARDID_4, 0)));

                return Arrays.asList(shard0, shard1, shard2, shard3, shard4);
            }

            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                if (request.getShardId().equals(SHARDID_0))
                {
                    fail("should not have queried shard 0");
                }
                else if (request.getShardId().equals(SHARDID_1))
                {
                    assertEquals("shard 1 iterator type",   ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                }
                else if (request.getShardId().equals(SHARDID_2))
                {
                    fail("should not have queried shard 2");
                }
                else if (request.getShardId().equals(SHARDID_3))
                {
                    assertEquals("shard 3 iterator type",   ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
                }
                else if (request.getShardId().equals(SHARDID_4))
                {
                    assertEquals("shard 4 iterator type",   ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
                    assertEquals("shard 4 sequence number", sequenceNumber4,                         request.getStartingSequenceNumber());
                }
                else
                {
                    fail("unexpected iterator request: " + request.getShardId());
                }
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(SHARDID_4, sequenceNumber4);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).withInitialSequenceNumbers(offsets);

        assertEquals("records from first read",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_1, RECORDS_3, RECORDS_4.subList(1, RECORDS_4.size())),
                     retrieveRecords(reader));

        assertEquals("records from second read",
                     Collections.emptyList(),
                     retrieveRecords(reader));
    }


    @Test
    public void testThrottling() throws Exception
    {
        myLogger.info("testThrottling");

        final AtomicInteger getRecordsInvocationCount = new AtomicInteger(0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1)
        {
            @Override
            public GetRecordsResult getRecords(GetRecordsRequest request)
            {
                if ((getRecordsInvocationCount.getAndIncrement() % 2) == 0)
                    throw new ProvisionedThroughputExceededException("testing");

                return super.getRecords(request);
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records returned",                        Collections.emptyList(), firstRead);
        assertEquals("after first read, number of calls to GetRecords",     1, getRecordsInvocationCount.get());

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("second read, records returned",                       RECORDS_1, secondRead);
        assertEquals("after second read, number of calls to GetRecords",    2, getRecordsInvocationCount.get());

        assertLogMessages("throttling log messages", "provisioned throughput exceeded", 1, Level.WARN);
    }


    @Test
    public void testExpiredIterator() throws Exception
    {
        myLogger.info("testExpiredIterator");

        final AtomicInteger getRecordsInvocationCount = new AtomicInteger(0);

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1)
        {
            @Override
            public GetRecordsResult getRecords(GetRecordsRequest request)
            {
                if ((getRecordsInvocationCount.getAndIncrement() % 2) == 0)
                    throw new ExpiredIteratorException("testing");

                return super.getRecords(request);
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon();

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records returned",                        Collections.emptyList(), firstRead);
        assertEquals("after first read, number of calls to GetRecords",     1, getRecordsInvocationCount.get());

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("second read, records returned",                       RECORDS_1, secondRead);
        assertEquals("after second read, number of calls to GetRecords",    2, getRecordsInvocationCount.get());

        assertLogMessages("expired iterator log messages", "iterator expired", 1, Level.WARN);
    }


    @Test
    public void testTimeoutRetrievingShardIterators() throws Exception
    {
        myLogger.info("testTimeoutRetrievingShardIterators");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1, RECORDS_2)
        {
            @Override
            public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request)
            {
                throw new ProvisionedThroughputExceededException("testing");
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon().withTimeout(50);

        List<String> firstRead = retrieveRecords(reader);
        assertEquals("first read, records returned", Collections.emptyList(), firstRead);

        List<String> secondRead = retrieveRecords(reader);
        assertEquals("first read, records returned", Collections.emptyList(), secondRead);

        assertLogMessages("unable to retrieve iterators log messages", "unable to retrieve shard iterators", 2, Level.WARN);
    }


    @Test
    public void testMillisBehindLatest() throws Exception
    {
        myLogger.info("testMillisBehindLatest");

        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_0, RECORDS_1)
        {
            LinkedList<Long> millisByRequest = new LinkedList<Long>(Arrays.asList(350L, 600L));

            @Override
            public GetRecordsResult getRecords(GetRecordsRequest request)
            {
                return super.getRecords(request)
                       .withMillisBehindLatest(millisByRequest.removeFirst());
            }

        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME).readFromTrimHorizon().withTimeout(50);

        assertEquals("value before iteration", 0, reader.getMillisBehindLatest());

        retrieveRecords(reader);

        assertEquals("value after all records read", 600, reader.getMillisBehindLatest());

        reader.iterator();

        assertEquals("value after creating new iterator", 0, reader.getMillisBehindLatest());
    }
}
