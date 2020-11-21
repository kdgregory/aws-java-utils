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

package com.kdgregory.aws.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.slf4j.MDC;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.lang.StringUtil;
import net.sf.kdgcommons.test.NumericAsserts;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.util.BinaryUtils;

import com.kdgregory.aws.utils.kinesis.KinesisReader;
import com.kdgregory.aws.utils.kinesis.KinesisUtil;
import com.kdgregory.aws.utils.kinesis.KinesisWriter;


/**
 *  A combined test for the Kinesis utilities: creates a stream, writes some messages
 *  to it, reads some messages, and then deletes the stream. Also tests increasing and
 *  decreasing the number of shards in the stream, and verifies that the reader will
 *  see an unbroken set of messages.
 *  <p>
 *  Note: does not clean up after exceptions.
 */
public class KinesisIntegrationTest
{
    private Log logger = LogFactory.getLog(getClass());

    // single client will be shared between all tests
    private static AmazonKinesis client;

    private String streamName;

//----------------------------------------------------------------------------
//  Setup/teardown
//----------------------------------------------------------------------------

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        client = AmazonKinesisClientBuilder.defaultClient();
    }


    @AfterClass
    public static void afterClass() throws Exception
    {
        client.shutdown();
    }
    
    
    /**
     *  Per-test initialization. Since each test is named, this isn't a @Setup
     *  method. Pass non-zero numShards to create the stream.
     */
    private void init(String testName, int numShards)
    {
        streamName = "KinesisIntegrationTest-" + testName + "-" + UUID.randomUUID().toString();

        MDC.put("testName", testName);
        logger.info("starting. stream name is " + streamName);
        
        if (numShards > 0)
        {
            KinesisUtil.createStream(client, streamName, numShards, 30000);
        }
    }
    
    
    @After
    public void tearDown() throws Exception
    {
        if (KinesisUtil.describeStream(client, streamName, 1000) != null)
        {
            KinesisUtil.deleteStream(client, streamName, 1000);
            // we don't bother waiting, so check your streams afterward!
        }

        logger.info("finished");
        MDC.clear();
    }
    
//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------
    
    /**
     *  Writes a set of messages to the stream. Assumes that all messages can
     *  be sent in a single batch.
     */
    private void writeMessages(String partitionKey, String message, int count)
    throws Exception
    {
        KinesisWriter writer = new KinesisWriter(client, streamName);
        for (int ii = 0 ; ii < count ; ii++)
        {
            writer.addRecord(partitionKey, message);
        }

        writer.sendAll(2000);
    }

    
    /**
     *  Reads up to an expected number of messages from the stream, using the
     *  specified writer (which may have been used before), timing out after
     *  10 seconds.
     */
    private List<Record> readMessages(KinesisReader reader, int expectedMessages)
    throws Exception
    {
        List<Record> result = new ArrayList<Record>();
        long timeoutAt = System.currentTimeMillis() + 10000;

        while ((result.size() < expectedMessages) && (System.currentTimeMillis() < timeoutAt))
        {
            for (Record record : reader)
            {
                result.add(record);
            }
        }

        return result;
    }


    /**
     *  Extracts the string values from one or more lists of records (used to
     *  combine records from multi-read tests, as well as for single-read tests).
     */
    private static List<String> extractText(List<Record>... batches)
    throws Exception
    {
        List<String> result = new ArrayList<String>();
        for (List<Record> batch : batches)
        {
            for (Record record : batch)
            {
                result.add(new String(BinaryUtils.copyBytesFrom(record.getData()), "UTF-8"));
            }
        }
        return result;
    }


    /**
     *  Asserts that a batch of messages contains the expected text values and expected
     *  number of partition keys (represented as a range to account for possible
     *  duplication of random keys).
     */
    private void assertRecords(
        String baseMessage, List<Record> records,
        int expectedRecordCount, int expectedMinPartitionKeys, int expectedMaxPartitionKeys,
        Set<String> expectedMessages)
    throws Exception
    {
        assertEquals(baseMessage + ": record count", expectedRecordCount, records.size());

        Set<String> partitionKeys = new HashSet<String>();
        Set<String> messages = new HashSet<String>();
        for (Record record : records)
        {
            partitionKeys.add(record.getPartitionKey());
            messages.add(new String(BinaryUtils.copyBytesFrom(record.getData()), "UTF-8"));
        }

        NumericAsserts.assertInRange(baseMessage + ": number of distinct partition keys",
                                     expectedMinPartitionKeys, expectedMaxPartitionKeys,
                                     partitionKeys.size());
        assertEquals(baseMessage + ": message contents", expectedMessages, messages);
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testCreateDescribeDestroy() throws Exception
    {
        init("testCreateDescribeDestroy", 0);

        logger.info("creating stream");
        KinesisUtil.createStream(client, streamName, 2, 60000);

        DescribeStreamRequest describeRequest = new DescribeStreamRequest().withStreamName(streamName);
        StreamDescription streamDesc = KinesisUtil.describeStream(client, describeRequest, 2000);
        assertEquals("stream status after creation", StreamStatus.ACTIVE, KinesisUtil.getStatus(streamDesc));
        
        logger.info("retrieving shards");
        List<Shard> shards = KinesisUtil.describeShards(client, streamName, 20000);
        assertEquals("returned expected number of shards", 2, shards.size());

        logger.info("deleting stream");
        KinesisUtil.deleteStream(client, streamName, 1000);
        assertNull("stream status after deletion", KinesisUtil.waitForStatus(client, streamName, null, 60000));
    }
    
    
    @Test
    public void testBasicWritingAndReading() throws Exception
    {
        init("testBasicWritingAndReading", 1);

        DescribeStreamRequest describeRequest = new DescribeStreamRequest().withStreamName(streamName);
        StreamDescription streamDesc = KinesisUtil.describeStream(client, describeRequest, 2000);
        assertEquals("stream status after creation", StreamStatus.ACTIVE, KinesisUtil.getStatus(streamDesc));

        logger.info("writing messages");
        writeMessages("foo", "bar", 10);
        writeMessages("foo", "baz", 10);
        writeMessages("foo", "bar", 10);
        writeMessages("foo", "baz", 10);

        logger.info("reading messages");
        KinesisReader reader = new KinesisReader(client, streamName).readFromTrimHorizon();
        List<Record> records = readMessages(reader, 41);    // this should time-out but read all messages

        assertRecords("all messages", records, 40, 1, 1, CollectionUtil.asSet("bar", "baz"));
    }


    @Test
    public void testIncreaseShardCount() throws Exception
    {
        init("testIncreaseShardCount", 2);

        logger.info("writing and reading initial batches of messages");
        writeMessages(null, "init 1", 10);
        writeMessages(null, "init 2", 10);
        writeMessages(null, "init 3", 10);

        KinesisReader reader = new KinesisReader(client, streamName).readFromTrimHorizon();
        List<Record> initialBatch = readMessages(reader, 30);

        assertRecords("initial batch", initialBatch, 30, 28, 30, CollectionUtil.asSet("init 1", "init 2", "init 3"));

        Map<String,String> savedSequenceNumbers = reader.getCurrentSequenceNumbers();

        logger.info("resharding stream");
        StreamStatus reshardStatus = KinesisUtil.reshard(client, streamName, 3, 300 * 1000);
        assertEquals("reshard successful", StreamStatus.ACTIVE, reshardStatus);

        logger.info("writing and reading final batches of messages");
        writeMessages(null, "final 1", 10);
        writeMessages(null, "final 2", 10);
        writeMessages(null, "final 3", 10);

        List<Record> finalBatch = readMessages(reader, 30);
        assertRecords("final batch", finalBatch, 30, 28, 30, CollectionUtil.asSet("final 1", "final 2", "final 3"));

        logger.info("second reader, from saved sequence numbers");
        KinesisReader reader2 = new KinesisReader(client, streamName).withInitialSequenceNumbers(savedSequenceNumbers);
        assertRecords("secondreader", readMessages(reader2, 60),
                      30, 28, 30,
                      CollectionUtil.asSet("final 1", "final 2", "final 3"));

        logger.info("third reader, reading entire stream");
        KinesisReader reader3 = new KinesisReader(client, streamName).readFromTrimHorizon();
        assertRecords("third reader", readMessages(reader3, 60),
                      60, 56, 60,
                      CollectionUtil.asSet("init 1", "init 2", "init 3", "final 1", "final 2", "final 3"));
    }


    @Test
    public void testDecreaseShardCount() throws Exception
    {
        init("testDecreaseShardCount", 3);

        logger.info("writing and reading initial batches of messages");
        writeMessages(null, "init 1", 10);
        writeMessages(null, "init 2", 10);
        writeMessages(null, "init 3", 10);

        KinesisReader reader = new KinesisReader(client, streamName).readFromTrimHorizon();
        List<Record> initialBatch = readMessages(reader, 30);

        assertRecords("initial batch", initialBatch, 30, 28, 30, CollectionUtil.asSet("init 1", "init 2", "init 3"));

        Map<String,String> savedSequenceNumbers = reader.getCurrentSequenceNumbers();

        logger.info("resharding stream");
        StreamStatus reshardStatus = KinesisUtil.reshard(client, streamName, 2, 300000);
        assertEquals("reshard successful", StreamStatus.ACTIVE, reshardStatus);

        logger.info("writing and reading final batches of messages");
        writeMessages(null, "final 1", 10);
        writeMessages(null, "final 2", 10);
        writeMessages(null, "final 3", 10);

        List<Record> finalBatch = readMessages(reader, 30);
        assertRecords("final batch", finalBatch, 30, 28, 30, CollectionUtil.asSet("final 1", "final 2", "final 3"));

        logger.info("second reader, from saved sequence numbers");
        KinesisReader reader2 = new KinesisReader(client, streamName).withInitialSequenceNumbers(savedSequenceNumbers);
        assertRecords("secondreader", readMessages(reader2, 60),
                      30, 28, 30,
                      CollectionUtil.asSet("final 1", "final 2", "final 3"));

        logger.info("third reader, reading entire stream");
        KinesisReader reader3 = new KinesisReader(client, streamName).readFromTrimHorizon();
        assertRecords("third reader", readMessages(reader3, 60),
                      60, 56, 60,
                      CollectionUtil.asSet("init 1", "init 2", "init 3", "final 1", "final 2", "final 3"));
    }


    @Test
    public void testLargeRecords() throws Exception
    {
        init("testLargeRecords", 1);

        KinesisWriter writer = new KinesisWriter(client, streamName);

        logger.info("writing messages");

        String messageBase = StringUtil.repeat('A', 1024 * 1024);
        assertTrue(writer.addRecord(messageBase.substring(0,  1), messageBase.substring(1)));
        assertTrue(writer.addRecord(messageBase.substring(0,  2), messageBase.substring(2)));
        assertTrue(writer.addRecord(messageBase.substring(0,  3), messageBase.substring(3)));
        assertTrue(writer.addRecord(messageBase.substring(0,  4), messageBase.substring(4)));
        assertTrue(writer.addRecord(messageBase.substring(0,  5), messageBase.substring(5)));

        // note: while we can attempt to send 5 MB in a single message, Kinesis will throttle
        // us to 1 MB per shard, so this will actually result in multiple send attempts

        writer.sendAll(10000);
        assertEquals("was able to send all records", 0, writer.getUnsentRecords().size());

        logger.info("reading all messages");
        KinesisReader reader = new KinesisReader(client, streamName).readFromTrimHorizon();
        List<Record> records = readMessages(reader, 5);

        assertEquals("number of messages read", 5, records.size());

        // we'll just spot-check the last record
        List<String> recordsAsText = extractText(records);
        assertEquals("retrieved entire message + partition key",
                     messageBase,
                     records.get(4).getPartitionKey() + recordsAsText.get(4));
    }
}
