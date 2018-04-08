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
import java.util.TreeSet;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.sf.kdgcommons.lang.StringUtil;
import net.sf.kdgcommons.test.NumericAsserts;
import net.sf.kdgcommons.test.StringAsserts;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.util.BinaryUtils;

import com.kdgregory.aws.utils.kinesis.KinesisReader;
import com.kdgregory.aws.utils.kinesis.KinesisUtils;
import com.kdgregory.aws.utils.kinesis.KinesisWriter;


/**
 *  A combined test for the Kinesis utilities: creates a stream, writes some messages
 *  to it, reads some messages, and then deletes the stream. Also tests increasing and
 *  decreasing the number of shards in the stream, and verifies that the reader will
 *  see an unbroken set of messages.
 *  <p>
 *  Note: does not clean up after exceptions.
 */
public class TestKinesis
{
    private Log logger = LogFactory.getLog(getClass());

    private AmazonKinesis client;
    private String streamName;

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    /**
     *  Writes a set of messages to the stream. Assumes that all messages can
     *  be sent in a single batch.
     */
    private void writeMessages(String partitionKey, String baseMessage, int count)
    throws Exception
    {
        KinesisWriter writer = new KinesisWriter(client, streamName);
        for (int ii = 0 ; ii < count ; ii++)
        {
            writer.addRecord(partitionKey, baseMessage + ": " + ii);
        }

        while (writer.getUnsentRecords().size() > 0)
        {
            writer.send();
            if (writer.getUnsentRecords().size() > 0)
            {
                Thread.sleep(250);
            }
        }
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

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testBasicOperation() throws Exception
    {
        logger.info("testBasicOperation");

        client = AmazonKinesisClientBuilder.defaultClient();
        streamName = "TestKinesis-testBasicOperation";

        logger.debug("testBasicOperation: creating stream");
        KinesisUtils.createStream(client, streamName, 1, 30000);

        DescribeStreamRequest describeRequest = new DescribeStreamRequest().withStreamName(streamName);
        StreamDescription streamDesc = KinesisUtils.describeStream(client, describeRequest, 2000);
        assertEquals("stream status after creation", StreamStatus.ACTIVE, KinesisUtils.getStatus(streamDesc));

        logger.debug("testBasicOperation: writing messages in two batches");
        writeMessages("foo", "bar", 10);
        writeMessages("foo", "baz", 10);

        logger.debug("testBasicOperation: reading all messages");
        KinesisReader reader = new KinesisReader(client, streamName).readFromTrimHorizon();
        List<Record> records = readMessages(reader, 20);

        assertEquals("number of messages read", 20, records.size());
        for (Record record : records)
        {
            String text = new String(BinaryUtils.copyBytesFrom(record.getData()), "UTF-8");
            assertEquals("partition key", "foo", record.getPartitionKey());
            StringAsserts.assertRegex("message text (was: " + text + ")", "ba[rz].*\\d", text);
        }

        assertFalse("after read, offsets should not be empty", reader.getCurrentSequenceNumbers().isEmpty());

        logger.debug("testBasicOperation: deleting stream");
        KinesisUtils.deleteStream(client, streamName, 1000);
        assertNull("stream was deleted", KinesisUtils.waitForStatus(client, streamName, null, 60000));
    }


    @Test
    public void testIncreaseShardCount() throws Exception
    {
        logger.info("testIncreaseShardCount");

        client = AmazonKinesisClientBuilder.defaultClient();
        streamName = "TestKinesis-testIncreaseShardCount";

        logger.debug("testIncreaseShardCount: creating stream");
        KinesisUtils.createStream(client, streamName, 2, 60000);

        logger.debug("testIncreaseShardCount: writing initial batches of messages");
        writeMessages(null, "init 1", 10);
        writeMessages(null, "init 2", 10);

        logger.debug("testIncreaseShardCount: reading initial messages");
        KinesisReader reader = new KinesisReader(client, streamName).readFromTrimHorizon();

        List<Record> initialBatch = readMessages(reader, 20);
        assertEquals("initial batch, number of messages read", 20, initialBatch.size());

        Set<String> distinctPartitionKeys = new HashSet<String>();
        for (Record record : initialBatch)
        {
            distinctPartitionKeys.add(record.getPartitionKey());
            String text = new String(BinaryUtils.copyBytesFrom(record.getData()), "UTF-8");
            StringAsserts.assertRegex("message text (was: " + text + ")", "init.*\\d", text);
        }
        NumericAsserts.assertInRange("number of distinct partition keys", 18, 20, distinctPartitionKeys.size());

        Map<String,String> savedSequenceNumbers = reader.getCurrentSequenceNumbers();
        assertEquals("saved sequence numbers indicates both shards read", 2, savedSequenceNumbers.size());

        logger.debug("testIncreaseShardCount: resharding stream");
        client.updateShardCount(new UpdateShardCountRequest().withStreamName(streamName)
                                .withTargetShardCount(3).withScalingType(ScalingType.UNIFORM_SCALING));

        logger.debug("testIncreaseShardCount: writing and reading messages while stream is being resharded");
        writeMessages(null, "middle", 10);
        List<Record> secondBatch = readMessages(reader, 10);
        assertEquals("number of messages written/read during reshard", 10, secondBatch.size());

        logger.debug("testIncreaseShardCount: waiting for reshard to complete");
        assertEquals("stream became active", StreamStatus.ACTIVE, KinesisUtils.waitForStatus(client, streamName, StreamStatus.ACTIVE, 180000));

        logger.debug("testIncreaseShardCount: writing and reading last batch of messages");
        writeMessages(null, "final", 10);
        List<Record> thirdBatch = readMessages(reader, 10);
        assertEquals("number of messages written/read after reshard", 10, thirdBatch.size());
        assertEquals("final offsets correspond to new shard count", 3, reader.getCurrentSequenceNumbers().size());

        logger.debug("testIncreaseShardCount: second reader, reading entire stream");
        KinesisReader reader2 = new KinesisReader(client, streamName).readFromTrimHorizon();
        List<Record> reader2Messages = readMessages(reader2, 40);

        assertEquals("reader2 read entire stream", 40, reader2Messages.size());
        assertEquals("reader2 read same messages as reader1",
                     new TreeSet<String>(extractText(initialBatch, secondBatch, thirdBatch)),
                     new TreeSet<String>(extractText(reader2Messages)));

        logger.debug("testIncreaseShardCount: third reader, reading from saved offsets");
        KinesisReader reader3 = new KinesisReader(client, streamName).withInitialSequenceNumbers(savedSequenceNumbers);
        List<Record> reader3Messages = readMessages(reader3, 20);
        assertEquals("reader3 read messages not read by reader1",
                     new TreeSet<String>(extractText(secondBatch, thirdBatch)),
                     new TreeSet<String>(extractText(reader3Messages)));

        logger.debug("testIncreaseShardCount: deleting stream");
        KinesisUtils.deleteStream(client, streamName, 1000);
        assertNull("stream was deleted", KinesisUtils.waitForStatus(client, streamName, null, 60000));
    }


    @Test
    public void testDecreaseShardCount() throws Exception
    {
        logger.info("testDecreaseShardCount");

        client = AmazonKinesisClientBuilder.defaultClient();
        streamName = "TestKinesis-testDecreaseShardCount";

        logger.debug("testDecreaseShardCount: creating stream");
        KinesisUtils.createStream(client, streamName, 3, 60000);

        logger.debug("testDecreaseShardCount: writing initial batches of messages");
        writeMessages(null, "init 1", 10);
        writeMessages(null, "init 2", 10);

        logger.debug("testDecreaseShardCount: reading initial messages");
        KinesisReader reader = new KinesisReader(client, streamName).readFromTrimHorizon();

        List<Record> initialBatch = readMessages(reader, 20);
        assertEquals("initial batch, number of messages read", 20, initialBatch.size());

        Set<String> distinctPartitionKeys = new HashSet<String>();
        for (Record record : initialBatch)
        {
            distinctPartitionKeys.add(record.getPartitionKey());
            String text = new String(BinaryUtils.copyBytesFrom(record.getData()), "UTF-8");
            StringAsserts.assertRegex("message text (was: " + text + ")", "init.*\\d", text);
        }
        NumericAsserts.assertInRange("number of distinct partition keys", 18, 20, distinctPartitionKeys.size());

        Map<String,String> savedSequenceNumbers = reader.getCurrentSequenceNumbers();
        assertEquals("saved sequence numbers indicates all shards read", 3, savedSequenceNumbers.size());

        logger.debug("testDecreaseShardCount: resharding stream");
        client.updateShardCount(new UpdateShardCountRequest().withStreamName(streamName)
                                .withTargetShardCount(2).withScalingType(ScalingType.UNIFORM_SCALING));

        logger.debug("testDecreaseShardCount: writing and reading messages while stream is being resharded");
        writeMessages(null, "middle", 10);
        List<Record> secondBatch = readMessages(reader, 10);
        assertEquals("number of messages written/read during reshard", 10, secondBatch.size());

        logger.debug("testDecreaseShardCount: waiting for reshard to complete");
        assertEquals("stream became active", StreamStatus.ACTIVE, KinesisUtils.waitForStatus(client, streamName, StreamStatus.ACTIVE, 180000));

        logger.debug("testDecreaseShardCount: writing and reading last batch of messages");
        writeMessages(null, "final", 10);
        List<Record> thirdBatch = readMessages(reader, 10);
        assertEquals("number of messages written/read after reshard", 10, thirdBatch.size());

        // TODO - this fails because we don't remove shard 2 (it's not an ancestor of a resulting shard)
        // assertEquals("final offsets correspond to new shard count", 2, reader.getCurrentSequenceNumbers().size());

        logger.debug("testDecreaseShardCount: second reader, reading entire stream");
        KinesisReader reader2 = new KinesisReader(client, streamName).readFromTrimHorizon();
        List<Record> reader2Messages = readMessages(reader2, 40);

        assertEquals("reader2 read entire stream", 40, reader2Messages.size());
        assertEquals("reader2 read same messages as reader1",
                     new TreeSet<String>(extractText(initialBatch, secondBatch, thirdBatch)),
                     new TreeSet<String>(extractText(reader2Messages)));

        logger.debug("testDecreaseShardCount: third reader, reading from saved offsets");
        KinesisReader reader3 = new KinesisReader(client, streamName).withInitialSequenceNumbers(savedSequenceNumbers);
        List<Record> reader3Messages = readMessages(reader3, 20);
        assertEquals("reader3 read messages not read by reader1",
                     new TreeSet<String>(extractText(secondBatch, thirdBatch)),
                     new TreeSet<String>(extractText(reader3Messages)));

        logger.debug("testDecreaseShardCount: deleting stream");
        KinesisUtils.deleteStream(client, streamName, 1000);
        assertNull("stream was deleted", KinesisUtils.waitForStatus(client, streamName, null, 60000));
    }


    @Test
    public void testLargeRecords() throws Exception
    {
        logger.info("testLargeRecords");

        client = AmazonKinesisClientBuilder.defaultClient();
        streamName = "TestKinesis-testLargeRecords";

        logger.debug("testLargeRecords: creating stream");
        KinesisUtils.createStream(client, streamName, 1, 30000);

        KinesisWriter writer = new KinesisWriter(client, streamName);

        logger.debug("testLargeRecords: writing messages");

        String messageBase = StringUtil.repeat('A', 1024 * 1024);
        assertTrue(writer.addRecord(messageBase.substring(0,  1), messageBase.substring(1)));
        assertTrue(writer.addRecord(messageBase.substring(0,  2), messageBase.substring(2)));
        assertTrue(writer.addRecord(messageBase.substring(0,  3), messageBase.substring(3)));
        assertTrue(writer.addRecord(messageBase.substring(0,  4), messageBase.substring(4)));
        assertTrue(writer.addRecord(messageBase.substring(0,  5), messageBase.substring(5)));

        // TODO - replace by sendAll()
        // note: while we can attempt to send 5 MB in a single message, we're limited to 1 MB
        // written to a single shard, so will actually loop here trying to resend
        int tryCount = 0;
        while ((tryCount++ < 10) && (writer.getUnsentRecords().size() > 0))
        {
            writer.send();
            Thread.sleep(1000);
        }

        logger.debug("testLargeRecords: reading all messages");
        KinesisReader reader = new KinesisReader(client, streamName).readFromTrimHorizon();
        List<Record> records = readMessages(reader, 5);

        assertEquals("number of messages read", 5, records.size());

        // we'll just spot-check the last record
        List<String> recordsAsText = extractText(records);
        assertEquals("retrieved entire message + partition key",
                     messageBase,
                     records.get(4).getPartitionKey() + recordsAsText.get(4));

        logger.debug("testLargeRecords: deleting stream");
        KinesisUtils.deleteStream(client, streamName, 1000);
        assertNull("stream was deleted", KinesisUtils.waitForStatus(client, streamName, null, 60000));
    }
}
