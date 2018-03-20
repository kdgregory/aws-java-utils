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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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

    private final static List<String> RECORDS_1 = Arrays.asList("foo", "bar");
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
            // TODO - handle different iterator types
            int offset = 0;
            return new GetShardIteratorResult()
                   .withShardIterator(formatSequenceNumber(request.getShardId(), offset));
        }

        @SuppressWarnings("unused")
        public GetRecordsResult getRecords(GetRecordsRequest request)
        {
            String shardItx = request.getShardIterator();
            String shardId = extractShardIdFromSeqnum(shardItx);
            int offset = extractOffsetFromSeqnum(shardItx);
            // TODO - actually use offset -- will limit records returned by each call
            List<Record> records = recordsByShard.get(shardId);
            return new GetRecordsResult()
                   .withRecords(records)
                   .withNextShardIterator(formatSequenceNumber(shardId, records.size()))
                   .withMillisBehindLatest(Long.valueOf(0));
        }
    }


//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testSingleShardNoOffsetsTrimHorizon() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1);
        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, ShardIteratorType.TRIM_HORIZON);

        List<String> retrievedRecords = new ArrayList<String>();
        for (Record record : reader)
        {
            byte[] recordData = BinaryUtils.copyAllBytesFrom(record.getData());
            retrievedRecords.add(new String(recordData, "UTF-8"));
            assertEquals("offsets have been updated",
                         record.getSequenceNumber(),
                         reader.getOffsets().get(extractShardIdFromSeqnum(record.getSequenceNumber())));
        }

        assertEquals("retrieved records", RECORDS_1, retrievedRecords);
    }


    @Test
    public void testMultipleShardsNoOffsetsTrimHorizon() throws Exception
    {
        KinesisMock mock = new KinesisMock(STREAM_NAME, RECORDS_1, RECORDS_2);
        KinesisReader reader = new KinesisReader(mock.getInstance(), STREAM_NAME, ShardIteratorType.TRIM_HORIZON);

        List<String> retrievedRecords = new ArrayList<String>();
        for (Record record : reader)
        {
            byte[] recordData = BinaryUtils.copyAllBytesFrom(record.getData());
            retrievedRecords.add(new String(recordData, "UTF-8"));
            assertEquals("offsets have been updated",
                         record.getSequenceNumber(),
                         reader.getOffsets().get(extractShardIdFromSeqnum(record.getSequenceNumber())));
        }

        assertEquals("retrieved records", CollectionUtil.combine(new ArrayList<String>(), RECORDS_1, RECORDS_2), retrievedRecords);
    }
}
