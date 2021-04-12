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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.*;
import org.apache.log4j.spi.*;

import net.sf.kdgcommons.lang.StringUtil;
import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


public class TestKinesisWriter
{
    private Logger myLogger = Logger.getLogger(getClass());

    private LinkedList<LoggingEvent> loggingEvents = new LinkedList<LoggingEvent>();

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        Logger writerLogger = Logger.getLogger(KinesisWriter.class);
        writerLogger.setLevel(Level.DEBUG);
        writerLogger.addAppender(new AppenderSkeleton()
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
//  Sample data
//----------------------------------------------------------------------------

    public final static String STREAM_NAME = "TestKinesisWriter";

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testHappyPath() throws Exception
    {
        myLogger.info("testHappyPath");

        KinesisMock mock = new KinesisMock(STREAM_NAME);
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        writer.addRecord("f\u00F6", "b\u00E4r");

        assertEquals("after adding first record, number of unsent records", 1, writer.getUnsentRecords().size());
        assertEquals("after adding first record, size of unsent records",   7, writer.getUnsentRecordSize());

        // this will be our spot-check to validate record contents
        assertEquals("record 0 partition key",      "f\u00F6",                      writer.getUnsentRecords().get(0).getPartitionKey());
        assertArrayEquals("record 0 data",          "b\u00E4r".getBytes("UTF-8"),   writer.getUnsentRecords().get(0).getData().array());

        writer.addRecord("argle", "bargle");

        assertEquals("after adding second record, number of unsent records", 2, writer.getUnsentRecords().size());
        assertEquals("after adding first record, size of unsent records",    18, writer.getUnsentRecordSize());

        List<PutRecordsRequestEntry> recordsToSend = writer.getUnsentRecords();

        writer.send();

        assertEquals("after send, number of result entries", 2, writer.getSendResults().size());
        assertEquals("after send, number of unsent records", 0, writer.getUnsentRecords().size());
        assertEquals("after send, size of unsent records",   0, writer.getUnsentRecordSize());

        assertSame("ordering of results: record 0", recordsToSend.get(0), writer.getSendResults().get(0).getRequestEntry());
        assertSame("ordering of results: record 1", recordsToSend.get(1), writer.getSendResults().get(1).getRequestEntry());
    }


    @Test
    public void testPartialThrottling() throws Exception
    {
        myLogger.info("testPartialThrottling");

        KinesisMock mock = new PartialThrottlingMock(STREAM_NAME, 2);
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        writer.addRecord("foo", "bar");

        assertEquals("after adding first record, number of unsent records", 1, writer.getUnsentRecords().size());
        assertEquals("after adding first record, size of unsent records",   6, writer.getUnsentRecordSize());

        writer.addRecord("baz", "biff");

        assertEquals("after adding second record, number of unsent records", 2, writer.getUnsentRecords().size());
        assertEquals("after adding first record, size of unsent records",    13, writer.getUnsentRecordSize());

        writer.addRecord("argle", "bargle");

        assertEquals("after adding third record, number of unsent records",  3, writer.getUnsentRecords().size());
        assertEquals("after adding third record, size of unsent records",    24, writer.getUnsentRecordSize());

        List<PutRecordsRequestEntry> recordsToSend = writer.getUnsentRecords();

        writer.send();

        assertEquals("after send, number of result entries", 3, writer.getSendResults().size());
        assertEquals("after send, number of unsent records", 1, writer.getUnsentRecords().size());
        assertEquals("after send, size of unsent records",   7, writer.getUnsentRecordSize());

        assertLogMessage(Level.WARN, " 1 .*" + STREAM_NAME + ".*ProvisionedThroughputExceeded");

        assertSame("ordering of results: record 0", recordsToSend.get(0), writer.getSendResults().get(0).getRequestEntry());
        assertSame("ordering of results: record 1", recordsToSend.get(1), writer.getSendResults().get(1).getRequestEntry());
        assertSame("ordering of results: record 2", recordsToSend.get(2), writer.getSendResults().get(2).getRequestEntry());
    }


    @Test
    public void testCompleteThrottling() throws Exception
    {
        myLogger.info("testCompleteThrottling");

        KinesisMock mock = new KinesisMock(STREAM_NAME)
        {
            @Override
            protected PutRecordsResultEntry processRecord(PutRecordsRequestEntry record, int index)
            {
                throw new ProvisionedThroughputExceededException("x");
            }
        };
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        writer.addRecord("foo", "bar");
        writer.addRecord("baz", "biff");

        assertEquals("before send, number of unsent records", 2, writer.getUnsentRecords().size());
        assertEquals("before send, size of unsent records",   13, writer.getUnsentRecordSize());

        writer.send();

        assertEquals("after send, number of result records", 0, writer.getSendResults().size());
        assertEquals("after send, number of unsent records", 2, writer.getUnsentRecords().size());
        assertEquals("after send, size of unsent records",   13, writer.getUnsentRecordSize());

        assertLogMessage(Level.WARN, "provisioned throughput exceeded.*" + STREAM_NAME);
    }


    @Test
    public void testMissingStream() throws Exception
    {
        myLogger.info("testMissingStream");

        KinesisMock mock = new KinesisMock(STREAM_NAME)
        {
            @Override
            protected PutRecordsResultEntry processRecord(PutRecordsRequestEntry record, int index)
            {
                throw new ResourceNotFoundException("x");
            }
        };
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        writer.addRecord("foo", "bar");
        writer.addRecord("baz", "biff");

        assertEquals("before send, number of unsent records", 2, writer.getUnsentRecords().size());
        assertEquals("before send, size of unsent records",   13, writer.getUnsentRecordSize());

        try
        {
            writer.send();
            fail("expected send to throw");
        }
        catch (ResourceNotFoundException ex)
        {
            assertEquals("after send, number of result records", 0, writer.getSendResults().size());
            assertEquals("after send, number of unsent records", 2, writer.getUnsentRecords().size());
            assertEquals("after send, size of unsent records",   13, writer.getUnsentRecordSize());
        }
    }


    @Test
    public void testEmptySend() throws Exception
    {
        myLogger.info("testEmptySend");

        KinesisMock mock = new KinesisMock(STREAM_NAME)
        {
            @Override
            public PutRecordsResult putRecords(PutRecordsRequest request)
            {
                // throwing anything will cause test to fail
                throw new ResourceNotFoundException("x");
            }
        };
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        writer.send();
    }


    @Test
    public void testClear() throws Exception
    {
        myLogger.info("testClear");

        KinesisMock mock = new KinesisMock(STREAM_NAME);
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        writer.addRecord("foo", "bar");
        writer.addRecord("baz", "biff");

        assertEquals("before clear, number of unsent records", 2, writer.getUnsentRecords().size());
        assertEquals("before clear, size of unsent records",   13, writer.getUnsentRecordSize());

        writer.clear();

        assertEquals("after clear, number of unsent records",   0, writer.getUnsentRecords().size());
        assertEquals("after clear, size of unsent records",     0, writer.getUnsentRecordSize());
    }


    @Test
    public void testOversizeMessage() throws Exception
    {
        myLogger.info("testOversizeMessage");

        KinesisMock mock = new KinesisMock(STREAM_NAME);
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        String partitionKey = "foo";
        String message = StringUtil.repeat('A', 1024 * 1024 - partitionKey.length());

        assertTrue("able to add message where partition key and data = 1 MB", writer.addRecord(partitionKey, message));

        writer.clear(); // not strictly necessary

        assertFalse("unable to add message where partition key and data > 1 MB", writer.addRecord(partitionKey, message + "x"));

        writer.clear(); // not strictly necessary

        assertFalse("message size is dependent on UTF-8 encoding of partition key", writer.addRecord(partitionKey.substring(1) + "\u00F6", message));
    }


    @Test
    public void testMaxRecordsInBatch() throws Exception
    {
        myLogger.info("testMaxRecordsInBatch");

        KinesisMock mock = new KinesisMock(STREAM_NAME);
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        for (int ii = 0 ; ii < 500 ; ii++)
        {
            assertTrue("able to add record " + ii, writer.addRecord("foo", "bar"));
        }

        assertFalse("unable to add > 500 records", writer.addRecord("foo", "bar"));
    }


    @Test
    public void testMaxBytesInBatch() throws Exception
    {
        myLogger.info("testMaxBytesInBatch");

        KinesisMock mock = new KinesisMock(STREAM_NAME);
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        String partitionKey = "foo";
        String bigMessage = StringUtil.repeat('A', 1024 * 1024 - partitionKey.length() - 1);

        for (int ii = 0 ; ii < 5 ; ii++)
        {
            assertTrue("able to add record " + ii, writer.addRecord(partitionKey, bigMessage));
        }

        assertEquals("after adding big messages", (5 * 1024 * 1024 - 5), writer.getUnsentRecordSize());

        assertFalse("unable to make request > 5 MB", writer.addRecord("foo", "bar"));
    }


    @Test
    public void testRandomPartitionKey() throws Exception
    {
        myLogger.info("testRandomPartitionKey");

        final Set<String> actualPartitionKeys = new HashSet<String>();

        KinesisMock mock = new KinesisMock(STREAM_NAME)
        {
            @Override
            protected PutRecordsResultEntry processRecord(PutRecordsRequestEntry record, int index)
            {
                actualPartitionKeys.add(record.getPartitionKey());
                return super.processRecord(record, index);
            }
        };
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        for (int ii = 0 ; ii < 5 ; ii++)
        {
            writer.addRecord(null, String.valueOf(ii));
            writer.addRecord("", String.valueOf(ii + 10));
        }

        writer.send();
        assertEquals("after send, number of unsent records", 0, writer.getUnsentRecords().size());

        // since we're using the standard Java random number generator we know that it
        // has a long cycle without repeats so this is a valid assertion
        assertEquals("number of distinct partition keys", 10, actualPartitionKeys.size());
    }


    @Test
    public void testSendAll() throws Exception
    {
        myLogger.info("testSendAll");

        KinesisMock mock = new PartialThrottlingMock(STREAM_NAME, 3);
        KinesisWriter writer = new KinesisWriter(mock.getInstance(), STREAM_NAME);

        for (int ii = 0 ; ii < 10 ; ii++)
        {
            writer.addRecord(String.valueOf(ii), "blah");
        }

        writer.sendAll(2000);

        assertEquals("result count",        14,     writer.getSendResults().size());
        assertEquals("unsent record count", 0,      writer.getUnsentRecords().size());
    }

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    /**
     *  Asserts that there's a logging message of a particular level that contains
     *  the expected text (specified as a regex).
     */
    private void assertLogMessage(Level level, String expectedMessageRegex)
    {
        Pattern pattern = Pattern.compile(expectedMessageRegex);
        for (LoggingEvent event : loggingEvents)
        {
            if ((event.getLevel() == level) && pattern.matcher(event.getRenderedMessage().toString()).find())
                return;
        }
        fail("did not find \"" + expectedMessageRegex + "\" in logging events");
    }


    /**
     *  A mock client that supports only <code>PutRecords</code>. By default,
     *  the request is recorded and response is successful. Override
     */
    public static class KinesisMock extends SelfMock<AmazonKinesis>
    {
        private String expectedStreamName;

        public List<PutRecordsRequest> recordsWritten = new ArrayList<PutRecordsRequest>();

        public KinesisMock(String expectedStreamName)
        {
            super(AmazonKinesis.class);
            this.expectedStreamName = expectedStreamName;
        }

        public PutRecordsResult putRecords(PutRecordsRequest request)
        {
            assertEquals("putRecords: stream name", expectedStreamName, request.getStreamName());

            List<PutRecordsResultEntry> results = new ArrayList<PutRecordsResultEntry>();
            int recordIndex = 0;
            int failedRecordCount = 0;
            for (PutRecordsRequestEntry requestEntry : request.getRecords())
            {
                PutRecordsResultEntry resultEntry = processRecord(requestEntry, recordIndex++);
                results.add(resultEntry);
                if (! StringUtil.isEmpty(resultEntry.getErrorCode()))
                    failedRecordCount++;
            }

            return new PutRecordsResult()
                   .withRecords(results)
                   .withFailedRecordCount(failedRecordCount);
        }

        protected PutRecordsResultEntry processRecord(PutRecordsRequestEntry record, int index)
        {
            return new PutRecordsResultEntry()
                   .withShardId("shard-0000")
                   .withSequenceNumber("00000000");
        }
    }


    /**
     *  A variant of the mock that fails 1 out of N records.
     */
    public static class PartialThrottlingMock extends KinesisMock
    {
        private int failureMod;

        public PartialThrottlingMock(String streamName, int failureMod)
        {
            super(streamName);
            this.failureMod = failureMod;
        }

        @Override
        protected PutRecordsResultEntry processRecord(PutRecordsRequestEntry record, int index)
        {
            // note: mod must not be 0 or we'll never end
            if (index % failureMod == 1)
            {
                return new PutRecordsResultEntry()
                       .withErrorCode("")
                       .withErrorCode("ProvisionedThroughputExceededException");
            }
            else
            {
                return super.processRecord(record, index);
            }
        }
    }
}
