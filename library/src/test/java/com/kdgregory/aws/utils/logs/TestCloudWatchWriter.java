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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Level;

import net.sf.kdgcommons.lang.StringUtil;
import static net.sf.kdgcommons.test.NumericAsserts.*;
import static net.sf.kdgcommons.test.StringAsserts.*;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAWSLogsClient;


public class TestCloudWatchWriter
{
    private Log4JCapturingAppender testLog;
    
    private void assertMessages(List<InputLogEvent> actual, String... expected)
    {
        assertEquals("number of messages", expected.length, actual.size());

        Iterator<InputLogEvent> eventItx = actual.iterator();
        for (int ii = 0 ; ii < expected.length ; ii++)
        {
            assertEquals("message " + ii, expected[ii], eventItx.next().getMessage());
        }
    }

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        testLog = Log4JCapturingAppender.getInstance();
        testLog.reset();
    }

//----------------------------------------------------------------------------
//  Tests
//----------------------------------------------------------------------------

    @Test
    public void testBasicOperation() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("bar"));
        AWSLogs client = mock.getInstance();

        CloudWatchWriter writer = new CloudWatchWriter(client, "foo", "bar");
        writer.add("appended first");
        writer.add(now - 1000, "appended second");
        writer.flush();

        assertEquals("describeStreams invocation count",    1,      mock.describeLogStreamsInvocationCount);
        assertEquals("putLogEvents invocation count",       1,      mock.putLogEventsInvocationCount);
        assertEquals("request included group name",         "foo",  mock.lastBatch.getLogGroupName());
        assertEquals("request included stream name",        "bar",  mock.lastBatch.getLogStreamName());
        assertFalse("request included sequence token",              StringUtil.isEmpty(mock.lastBatch.getSequenceToken()));

        assertMessages(mock.lastBatch.getLogEvents(), "appended second", "appended first");

        testLog.assertLogSize(0);
    }


    @Test
    public void testAutoCreateGroupAndStream() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("bar"));
        AWSLogs client = mock.getInstance();

        CloudWatchWriter writer = new CloudWatchWriter(client, "argle", "bargle");
        writer.add("appended first");
        writer.add(now - 1000, "appended second");
        writer.flush();

        assertEquals("describeStreams invocation count",    2,  mock.describeLogStreamsInvocationCount);
        assertEquals("createLogGroup invocation count",     1,  mock.createLogGroupInvocationCount);
        assertEquals("createLogStream invocation count",    1,  mock.createLogStreamInvocationCount);
        assertEquals("putLogEvents invocation count",       1,  mock.putLogEventsInvocationCount);
        assertFalse("request included sequence token",      StringUtil.isEmpty(mock.lastBatch.getSequenceToken()));

        assertMessages(mock.lastBatch.getLogEvents(), "appended second", "appended first");

        testLog.assertLogEntry(0, Level.DEBUG, "stream.*argle.*bargle.*does not exist.*");
        testLog.assertLogEntry(1, Level.DEBUG, "creating .* log stream.*bargle.*");
        testLog.assertLogEntry(2, Level.DEBUG, "creating .* log group.*argle.*");
    }


    @Test
    public void testBogusMessages() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("bar"));
        AWSLogs client = mock.getInstance();
        CloudWatchWriter writer = new CloudWatchWriter(client, "argle", "bargle");

        try
        {
            writer.add(System.currentTimeMillis() - (86400000 * 14 + 1), "bogus");
            fail("accepted message > 2 weeks old");
        }
        catch (IllegalArgumentException ex)
        {
            assertRegex("too-old message exception (was: " + ex.getMessage() + ")",
                        "message timestamp too far in past: [0-9]+ \\(limit: [0-9]+\\b\\)",
                        ex.getMessage());
        }

        try
        {
            writer.add(System.currentTimeMillis() + (3600000 * 2 + 1), "bogus");
            fail("accepted message > 2 hours in future");
        }
        catch (IllegalArgumentException ex)
        {
            assertRegex("future message exception (was: " + ex.getMessage() + ")",
                        "message timestamp too far in future: [0-9]+ \\(limit: [0-9]+\\b\\)",
                        ex.getMessage());
        }

        try
        {
            // we'll use a two-byte UTF-8 character to make things interesting
            String bogus = StringUtil.repeat('\u00C0', 512 * 1024 - 13) + "X";
            writer.add(bogus);
            fail("accepted too-large message");
        }
        catch (IllegalArgumentException ex)
        {
            assertRegex("too-large message exception (was: " + ex.getMessage() + ")",
                        "message is too large: 1048551 bytes \\(limit: 1048550\\)",
                        ex.getMessage());
        }
    }


    @Test
    public void testCountBasedBatching() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("bar"));
        AWSLogs client = mock.getInstance();
        CloudWatchWriter writer = new CloudWatchWriter(client, "foo", "bar");

        for (int ii = 0 ; ii < 18000 ; ii++)
        {
            // note that these will be written in reverse order
            writer.add(now - ii, String.valueOf(ii));
        }
        writer.flush();

        assertEquals("describeStreams invocation count",    1,          mock.describeLogStreamsInvocationCount);
        assertEquals("putLogEvents invocation count",       2,          mock.putLogEventsInvocationCount);
        assertEquals("all messages written",                18000,      mock.allMessages.size());
        assertEquals("first message written",               "17999",    mock.allMessages.get(0).getMessage());
        assertEquals("last message written",                "0",        mock.allMessages.get(17999).getMessage());
        assertEquals("last batch size",                     8000,       mock.lastBatch.getLogEvents().size());
        assertEquals("last batch first message",            "7999",     mock.lastBatch.getLogEvents().get(0).getMessage());
        assertEquals("last batch last message",             "0",        mock.lastBatch.getLogEvents().get(7999).getMessage());

        testLog.assertLogSize(0);
    }


    @Test
    public void testSizeBasedBatching() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("bar"));
        AWSLogs client = mock.getInstance();
        CloudWatchWriter writer = new CloudWatchWriter(client, "foo", "bar");

        // this format means that each record (including overhead) will be 1024 bytes
        String messageFormat = StringUtil.repeat('X', 993) + " %04d";

        for (int ii = 0 ; ii < 2000 ; ii++)
        {
            // note that these will be written in reverse order
            writer.add(now - ii, String.format(messageFormat, ii));
        }
        writer.flush();

        assertEquals("describeStreams invocation count",    1,          mock.describeLogStreamsInvocationCount);
        assertEquals("putLogEvents invocation count",       2,          mock.putLogEventsInvocationCount);
        assertEquals("all messages written",                2000,       mock.allMessages.size());
        assertRegex("first message written",                ".* 1999",  mock.allMessages.get(0).getMessage());
        assertRegex("last message written",                 ".* 0000",  mock.allMessages.get(1999).getMessage());
        assertEquals("last batch size",                     976,        mock.lastBatch.getLogEvents().size());
        assertRegex("last batch first message",             ".* 0975",  mock.lastBatch.getLogEvents().get(0).getMessage());
        assertRegex("last batch last message",              ".* 0000",  mock.lastBatch.getLogEvents().get(975).getMessage());

        testLog.assertLogSize(0);
    }


    @Test
    public void testTimestampBasedBatching() throws Exception
    {
        long now = System.currentTimeMillis();

        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("bar"));
        AWSLogs client = mock.getInstance();
        CloudWatchWriter writer = new CloudWatchWriter(client, "foo", "bar");

        for (int ii = 0 ; ii < 1500 ; ii++)
        {
            // note that these will be written in reverse order
            writer.add(now - ii * 60000, String.valueOf(ii));
        }
        writer.flush();

        assertEquals("describeStreams invocation count",    1,          mock.describeLogStreamsInvocationCount);
        assertEquals("putLogEvents invocation count",       2,          mock.putLogEventsInvocationCount);
        assertEquals("all messages written",                1500,       mock.allMessages.size());
        assertEquals("first message written",               "1499",     mock.allMessages.get(0).getMessage());
        assertEquals("last message written",                "0",        mock.allMessages.get(1499).getMessage());
        assertEquals("last batch size",                     60,         mock.lastBatch.getLogEvents().size());
        assertEquals("last batch first message",            "59",       mock.lastBatch.getLogEvents().get(0).getMessage());
        assertEquals("last batch last message",             "0",        mock.lastBatch.getLogEvents().get(59).getMessage());

        testLog.assertLogSize(0);
    }
}
