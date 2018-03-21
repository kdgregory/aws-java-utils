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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

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
//----------------------------------------------------------------------------
//  Some common test parameters
//----------------------------------------------------------------------------

    private final static String STREAM_NAME = "example";

    private final static List<String> RECORDS_1 = Arrays.asList("foo", "bar", "baz");
    private final static List<String> RECORDS_2 = Arrays.asList("argle", "bargle");


//----------------------------------------------------------------------------
//  Test Helpers
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
                         reader.getOffsets().get(extractShardIdFromSeqnum(record.getSequenceNumber())));
        }
        return retrievedRecords;
    }


    /**
     *  Uses reflection to extract the current shard iterators from the reader.
     */
    private Map<String,String> getShardIterators(KinesisReader reader)
    throws Exception
    {
        Field field = reader.getClass().getDeclaredField("shardIterators");
        field.setAccessible(true);
        return (Map<String,String>)field.get(reader);
    }


    /**
     *  A mock class that supports just enough behavior to make the reader work.
     *  Construct with one or more lists of strings that represent the records
     *  in a shard.
     *  <p>
     *  Note: to simplify mock implementation, shard iterators and sequence
     *  numbers use the same format.
     */
    private static class KinesisMock extends SelfMock<AmazonKinesis>
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

        @SuppressWarnings("unused")
        public DescribeStreamResult describeStream(DescribeStreamRequest request)
        {
            assertEquals("request contains stream name", expectedStreamName, request.getStreamName());

            List<Shard> shards = new ArrayList<Shard>();
            for (String shardId : recordsByShard.keySet())
            {
                shards.add(new Shard()
                           .withShardId(shardId)
                           .withSequenceNumberRange(new SequenceNumberRange()
                                                    .withStartingSequenceNumber(formatSequenceNumber(shardId, 0))));
            }

            return new DescribeStreamResult()
                       .withStreamDescription(new StreamDescription()
                           .withStreamName(expectedStreamName)
                           .withStreamStatus(StreamStatus.ACTIVE)
                           .withShards(shards)
                           .withHasMoreShards(Boolean.FALSE));
        }

        @SuppressWarnings("unused")
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

        @SuppressWarnings("unused")
        public GetRecordsResult getRecords(GetRecordsRequest request)
        {
            assertGetRecords(request);

            String shardItx = request.getShardIterator();
            String shardId = extractShardIdFromSeqnum(shardItx);
            int offset = extractOffsetFromSeqnum(shardItx);
            List<Record> shardRecords = recordsByShard.get(shardId);
            List<Record> remainingRecords = shardRecords.subList(offset, shardRecords.size());
            List<Record> returnedRecords = limitReturnedRecords(remainingRecords);
            String nextShardItx = (offset + returnedRecords.size() == shardRecords.size())
                                ? null
                                : formatSequenceNumber(shardId, offset + returnedRecords.size() - 1);

            return new GetRecordsResult()
                   .withRecords(returnedRecords)
                   .withNextShardIterator(nextShardItx)
                   .withMillisBehindLatest(Long.valueOf(0));
        }

        // hooks for subclasses

        protected void assertShardIteratorRequest(GetShardIteratorRequest request)
        {
            // default does nothing
        }

        protected void assertGetRecords(GetRecordsRequest request)
        {
            // default does nothing
        }

        protected List<Record> limitReturnedRecords(List<Record> records)
        {
            return records;
        }
    }


//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testSingleShardNoOffsetsTrimHorizon() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1)
        {
            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                assertEquals("request iterator type", ShardIteratorType.TRIM_HORIZON, ShardIteratorType.fromValue(request.getShardIteratorType()));
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, ShardIteratorType.TRIM_HORIZON);

        assertEquals("retrieved records", RECORDS_1, retrieveRecords(reader));
        assertNull("at end of stream, iterator is null", getShardIterators(reader).get(formatShardId(0)));
    }


    @Test
    public void testSingleShardNoOffsetsLatest() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1)
        {
            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                assertEquals("request iterator type", ShardIteratorType.LATEST, ShardIteratorType.fromValue(request.getShardIteratorType()));
            }
        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, ShardIteratorType.LATEST);

        assertEquals("retrieved records", Collections.emptyList(), retrieveRecords(reader));
        assertNull("at end of stream, iterator is null", getShardIterators(reader).get(formatShardId(0)));
    }


    @Test
    public void testSingleShardWithOffsets() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1)
        {
            @Override
            protected void assertShardIteratorRequest(GetShardIteratorRequest request)
            {
                assertEquals("request iterator type", ShardIteratorType.AFTER_SEQUENCE_NUMBER, ShardIteratorType.fromValue(request.getShardIteratorType()));
            }
        };

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(formatShardId(0), formatSequenceNumber(formatShardId(0), 0));
        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, offsets);

        assertEquals("retrieved records",
                     RECORDS_1.subList(1, RECORDS_1.size()),
                     retrieveRecords(reader));

        assertNull("at end of stream, iterator is null", getShardIterators(reader).get(formatShardId(0)));
    }

    @Test
    public void testSingleShardRepeatedRead() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1)
        {
            @Override
            protected List<Record> limitReturnedRecords(List<Record> records)
            {
                if (records.size() > 2)
                    records = records.subList(0, 2);
                return records;
            }

        };

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, ShardIteratorType.TRIM_HORIZON);

        assertEquals("first read",
                     RECORDS_1.subList(0, 2),
                     retrieveRecords(reader));
        assertEquals("after first read, iterator has been updated",
                     formatSequenceNumber(formatShardId(0), 1),
                     getShardIterators(reader).get(formatShardId(0)));

        assertEquals("second read",
                     RECORDS_1.subList(2, RECORDS_1.size()),
                     retrieveRecords(reader));
        assertNull("at end of stream, iterator is null", getShardIterators(reader).get(formatShardId(0)));
    }


    @Test
    public void testSingleShardOnceAtEndOfStreamDoesNotRepeat() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1);

        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, ShardIteratorType.TRIM_HORIZON);

        assertEquals("records from first read",     RECORDS_1,               retrieveRecords(reader));
        assertEquals("records from second read",    Collections.emptyList(), retrieveRecords(reader));
    }


    @Test
    public void testMultipleShardsNoOffsetsTrimHorizon() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1, RECORDS_2);
        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, ShardIteratorType.TRIM_HORIZON);

        assertEquals("retrieved records",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_1, RECORDS_2),
                     retrieveRecords(reader));

        assertNull("at end of streams, shard 0 iterator is null", getShardIterators(reader).get(formatShardId(0)));
        assertNull("at end of streams, shard 1 iterator is null", getShardIterators(reader).get(formatShardId(1)));
    }


    @Test
    public void testMultipleShardsWithOffsets() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1, RECORDS_2);

        Map<String,String> offsets = new HashMap<String,String>();
        offsets.put(formatShardId(0), formatSequenceNumber(formatShardId(0), 0));
        offsets.put(formatShardId(1), formatSequenceNumber(formatShardId(1), 0));
        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, offsets);

        assertEquals("retrieved records",
                     CollectionUtil.combine(new ArrayList<String>(), RECORDS_1.subList(1, RECORDS_1.size()),
                                                                     RECORDS_2.subList(1, RECORDS_2.size())),
                     retrieveRecords(reader));

        assertNull("at end of streams, shard 0 iterator is null", getShardIterators(reader).get(formatShardId(0)));
        assertNull("at end of streams, shard 1 iterator is null", getShardIterators(reader).get(formatShardId(1)));
    }
}
