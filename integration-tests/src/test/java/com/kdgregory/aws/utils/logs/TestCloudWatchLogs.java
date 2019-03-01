// Copyright (c) Keith D Gregory
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.kdgregory.aws.utils.logs;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.*;


/**
 *  Combined test for all CloudWatch Logs functionality.
 */
public class TestCloudWatchLogs
{
    // a single instance of the client is shared between all tests
    private static AWSLogs client;

//----------------------------------------------------------------------------
//  Pre/post operations
//----------------------------------------------------------------------------

    @BeforeClass
    public static void beforeClass()
    {
        client = AWSLogsClientBuilder.defaultClient();
    }


    @AfterClass
    public static void afterClass()
    {
        client.shutdown();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testCreationAndDeletion() throws Exception
    {

        String namePrefix = "TestLogsUtil-testBasicOperation";
        String logGroupName = namePrefix + "-logGroup-" + UUID.randomUUID();
        String logStreamName = namePrefix + "-logStream-" + UUID.randomUUID();

        // this will also exercise createLogGroup()
        assertNotNull("creation succeeded",                     CloudWatchLogsUtil.createLogStream(client, logGroupName, logStreamName, 30000));

        assertNotNull("post-create describeLogGroup()",         CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNotNull("post-create describeLogStream()",        CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));

        assertTrue("stream deletion succeeded",                  CloudWatchLogsUtil.deleteLogStream(client, logGroupName, logStreamName, 30000));

        assertNotNull("post-delete-stream describeLogGroup()",  CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNull("post-delete-stream describeLogStream()",    CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));

        assertTrue("group deletion succeeded",                  CloudWatchLogsUtil.deleteLogGroup(client, logGroupName, 30000));

        assertNull("post-delete-group describeLogGroup()",      CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNull("post-delete-group describeLogStream()",     CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));
    }


    @Test
    public void testWriterAndReader() throws Exception
    {
        int numRecords = 1000;
        int lastIndex = numRecords - 1;
        String namePrefix = "TestLogsUtil-testWriterAndReader";
        String logGroupName = namePrefix + "-logGroup-" + UUID.randomUUID();
        String logStreamName = namePrefix + "-logStream-" + UUID.randomUUID();

        long now = System.currentTimeMillis();

        CloudWatchLogsWriter writer = new CloudWatchLogsWriter(client, logGroupName, logStreamName);

        for (int ii = 0 ; ii < numRecords ; ii++)
        {
            writer.add(now - 100 * ii, String.format("message %04d", ii));
        }
        writer.flush();


        CloudWatchLogsReader reader = new CloudWatchLogsReader(client, logGroupName, logStreamName);
        List<OutputLogEvent> events1 = reader.retrieve(numRecords, 10000);

        assertEquals("retrieved all records",   numRecords,                     events1.size());

        assertEquals("first record timestamp",   now - 100 * lastIndex,         events1.get(0).getTimestamp().longValue());
        assertEquals("last record timestamp",    now,                           events1.get(lastIndex).getTimestamp().longValue());
        assertEquals("first record message",    "message 0999",                 events1.get(0).getMessage());
        assertEquals("last record's message",   "message 0000",                 events1.get(lastIndex).getMessage());

        // note: time range is inclusive start, exclusive end
        long timeStart = now - 100 * 50;
        long timeEnd   = now - 100 * 20;
        reader.withTimeRange(timeStart, timeEnd);
        List<OutputLogEvent> events2 = reader.retrieve();

        assertEquals("number of records in range",  30,                                         events2.size());
        assertEquals("first event in range",        events1.get(lastIndex - 50).getMessage(),   events2.get(0).getMessage());
        assertEquals("last event in rage",          events1.get(lastIndex - 21).getMessage(),   events2.get(29).getMessage());
    }


    @Test
    public void testWriterMultiThread() throws Exception
    {
        final int numThreads = 10;
        final int recordsPerThread = 1000;
        final int flushInterval = 200;
        final String namePrefix = "TestLogsUtil-testWriterMultiThread";
        final String logGroupName = namePrefix + "-logGroup-" + UUID.randomUUID();
        final String logStreamName = namePrefix + "-logStream-" + UUID.randomUUID();

        List<Thread> threads = new ArrayList<Thread>();
        for (int ii = 0 ; ii < numThreads ; ii++)
        {
            threads.add(new Thread(new Runnable()
            {
                @Override
                public void run()
                {

                    CloudWatchLogsWriter writer = new CloudWatchLogsWriter(client, logGroupName, logStreamName);
                    for (int jj = 0 ; jj < recordsPerThread ; jj++)
                    {
                        writer.add("message " + jj + " on thread " + Thread.currentThread().getId());
                        if ((jj % flushInterval) == 0)
                            writer.flush();
                    }
                    writer.flush();
                }
            }));
        }

        for (Thread thread : threads)
        {
            thread.start();
        }

        for (Thread thread : threads)
        {
            thread.join();
        }


        CloudWatchLogsReader reader = new CloudWatchLogsReader(client, logGroupName, logStreamName);
        List<OutputLogEvent> events = reader.retrieve(numThreads * recordsPerThread, 10000);

        assertEquals("retrieved all events", numThreads * recordsPerThread, events.size());
    }


    @Test
    public void testReaderMultiStream() throws Exception
    {
        int numRecords = 1000;
        String namePrefix = "TestLogsUtil-testWriterAndReader";
        String logGroupName = namePrefix + "-logGroup-" + UUID.randomUUID();
        String logStreamName1 = namePrefix + "-logStream-" + UUID.randomUUID();
        String logStreamName2 = namePrefix + "-logStream-" + UUID.randomUUID();

        long now = System.currentTimeMillis();

        CloudWatchLogsWriter writer1 = new CloudWatchLogsWriter(client, logGroupName, logStreamName1);
        CloudWatchLogsWriter writer2 = new CloudWatchLogsWriter(client, logGroupName, logStreamName2);

        for (int ii = 0 ; ii < numRecords ; ii++)
        {
            writer1.add(now - 100 * ii, String.format("message %04d", ii));
            writer2.add(now - 100 * ii, String.format("message %04d", ii));
        }
        writer1.flush();
        writer2.flush();

        CloudWatchLogsReader reader = new CloudWatchLogsReader(client, logGroupName, logStreamName1, logStreamName1);
        List<OutputLogEvent> events = reader.retrieve(numRecords * 2, 10000);

        assertEquals("retrieved all records", numRecords * 2, events.size());

        assertEquals("first two records have same timestamp (sorted together even though originating separately)",
                     events.get(0).getTimestamp(),
                     events.get(1).getTimestamp());
    }


}
