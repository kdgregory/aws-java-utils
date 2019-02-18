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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.logs.CloudWatchLogsUtil;
import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAWSLogsClient;


public class TestCloudWatchLogsUtil
{
    /**
     *  Asserts that a list of log groups contains all of the expected names.
     */
    public static void assertLogGroupNames(List<LogGroup> groups, String... names)
    {
        Set<String> lookup = new TreeSet<String>(Arrays.asList(names));
        for (LogGroup group : groups)
        {
            String name = group.getLogGroupName();
            assertTrue("unexpected log group: " + name, lookup.contains(name));
            lookup.remove(name); // duplicates will fail
        }

        assertTrue("missing log groups: " + lookup, lookup.isEmpty());
    }


    /**
     *  Asserts that a list of log streams contains all of the expected names.
     */
    public static void assertLogStreamNames(List<LogStream> streams, String... names)
    {
        Set<String> lookup = new TreeSet<String>(Arrays.asList(names));
        for (LogStream stream : streams)
        {
            String name = stream.getLogStreamName();
            assertTrue("unexpected log stream: " + name, lookup.contains(name));
            lookup.remove(name); // duplicates will fail
        }

        assertTrue("missing log streams: " + lookup, lookup.isEmpty());
    }

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        Log4JCapturingAppender.getInstance().reset();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testDescribeLogGroups() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar", "baz"),
                                                 Arrays.<String>asList());
        AWSLogs client = mock.getInstance();

        List<LogGroup> groups = CloudWatchLogsUtil.describeLogGroups(client, null);
        assertLogGroupNames(groups, "foo", "bar", "baz");

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testDescribeLogGroupsWithPrefix() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar", "baz"),
                                                 Arrays.<String>asList());
        AWSLogs client = mock.getInstance();

        List<LogGroup> groups = CloudWatchLogsUtil.describeLogGroups(client, "ba");
        assertLogGroupNames(groups, "bar", "baz");

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testDescribeLogGroupsWithPagination() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar", "baz"),
                                                 Arrays.<String>asList(),
                                                 2);
        AWSLogs client = mock.getInstance();

        List<LogGroup> groups = CloudWatchLogsUtil.describeLogGroups(client, "ba");
        assertLogGroupNames(groups, "bar", "baz");
        assertEquals("describeLogGroups invocation count", 2, mock.describeLogGroupsInvocationCount);

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testDescribeLogStreams() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar"),
                                                 Arrays.asList("argle", "bargle", "bazzle"));
        AWSLogs client = mock.getInstance();

        List<LogStream> streams = CloudWatchLogsUtil.describeLogStreams(client, "foo", null);
        assertLogStreamNames(streams, "argle", "bargle", "bazzle");

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testDescribeLogStreamsWithPrefix() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar"),
                                                 Arrays.asList("argle", "bargle", "bazzle"));
        AWSLogs client = mock.getInstance();

        List<LogStream> streams = CloudWatchLogsUtil.describeLogStreams(client, "foo", "ba");
        assertLogStreamNames(streams, "bargle", "bazzle");

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testDescribeLogStreamsWithPagination() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar"),
                                                 Arrays.asList("argle", "bargle", "bazzle"),
                                                 2);
        AWSLogs client = mock.getInstance();

        List<LogStream> streams = CloudWatchLogsUtil.describeLogStreams(client, "foo", "ba");
        assertLogStreamNames(streams, "bargle", "bazzle");
        assertEquals("describeLogStreams invocation count", 2, mock.describeLogStreamsInvocationCount);

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testDescribeLogStreamsWithMissingGroup() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar"),
                                                 Arrays.asList("argle", "bargle", "bazzle"),
                                                 2);
        AWSLogs client = mock.getInstance();

        List<LogStream> streams = CloudWatchLogsUtil.describeLogStreams(client, "fribble", "ba");
        assertLogStreamNames(streams);

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testWaitUntilGroupCreated() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar", "baz"),
                                                 Arrays.<String>asList())
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                if (describeLogGroupsInvocationCount++ < 5)
                    return new DescribeLogGroupsResult().withLogGroups();
                else
                    return super.describeLogGroups(request);
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.waitUntilCreated(client, "bar", 100, 10);
        long finish = System.currentTimeMillis();

        assertNotNull("found log group", group);
        assertTrue("made multiple describe attempts", mock.describeLogGroupsInvocationCount > 1);
        assertInRange("waited between attempts", 50, 100, (finish - start));

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testWaitUntilGroupCreatedTimeout() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar", "baz"),
                                                 Arrays.<String>asList());
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.waitUntilCreated(client, "biff", 100, 10);
        long finish = System.currentTimeMillis();

        assertNull("did not find log group",            group);
        assertTrue("made multiple describe attempts",               mock.describeLogGroupsInvocationCount > 1);
        assertInRange("waited between attempts",        80, 120,    (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("timeout expired.*log group.*biff", 0);
    }


    @Test
    public void testWaitUntilGroupCreatedPassingPrefix() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar", "baz"),
                                                 Arrays.<String>asList());
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.waitUntilCreated(client, "ba", 50, 10);
        long finish = System.currentTimeMillis();

        assertNull("did not find log group",            group);
        assertTrue("made multiple describe attempts",               mock.describeLogGroupsInvocationCount > 1);
        assertInRange("waited between attempts",        30, 80,     (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("timeout expired.*log group.*ba", 0);
    }


    @Test
    public void testWaitUntilStreamCreated() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar"),
                                                 Arrays.asList("argle", "bargle", "bazzle"))
        {
            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                if (describeLogStreamsInvocationCount++ < 5)
                    return new DescribeLogStreamsResult().withLogStreams();
                else
                    return super.describeLogStreams(request);
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogStream stream = CloudWatchLogsUtil.waitUntilCreated(client, "bar", "bargle", 100, 10);
        long finish = System.currentTimeMillis();

        assertNotNull("found log stream", stream);
        assertTrue("made multiple describe attempts", mock.describeLogStreamsInvocationCount > 1);
        assertInRange("waited between attempts", 50, 100, (finish - start));

        Log4JCapturingAppender.getInstance().assertNoLogEntries();
    }


    @Test
    public void testWaitUntilStreamCreatedTimeout() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar"),
                                                 Arrays.asList("argle", "bargle", "bazzle"));
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogStream stream = CloudWatchLogsUtil.waitUntilCreated(client, "foo", "biff", 100, 10);
        long finish = System.currentTimeMillis();

        assertNull("did not find log stream", stream);
        assertTrue("made multiple describe attempts", mock.describeLogStreamsInvocationCount > 1);
        assertInRange("waited between attempts", 80, 120, (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("timeout expired.*log stream.*biff", 0);
    }


    @Test
    public void testWaitUntilStreamCreatedPassingPrefix() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo", "bar"),
                                                 Arrays.asList("argle", "bargle", "bazzle"));
        AWSLogs client = mock.getInstance();

        LogStream stream = CloudWatchLogsUtil.waitUntilCreated(client, "foo", "ba", 50, 10);

        assertNull("did not find log stream", stream);

        Log4JCapturingAppender.getInstance().assertLogEntry("timeout expired.*log stream.*ba", 0);
    }


    @Test
    public void testCreateLogGroup() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                if (describeLogGroupsInvocationCount++ == 0)
                    return new DescribeLogGroupsResult().withLogGroups();
                else
                    return super.describeLogGroups(request);
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.createLogGroup(client, "bar", 100);
        long finish = System.currentTimeMillis();

        assertEquals("returned LogGroup object",            "bar",      group.getLogGroupName());
        assertEquals("createLogGroup invocation count",     1,          mock.createLogGroupInvocationCount);
        assertTrue("made multiple describes",                           mock.describeLogGroupsInvocationCount > 1);
        assertInRange("waited between describes",           20, 90,     (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("creating.*log group.*bar", 0);
    }


    @Test
    public void testCreateLogGroupAlreadyExists() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"));
        AWSLogs client = mock.getInstance();

        LogGroup group = CloudWatchLogsUtil.createLogGroup(client, "foo", 100);

        assertEquals("returned LogGroup object",            "foo",  group.getLogGroupName());
        assertEquals("createLogGroup invocation count",     1,      mock.createLogGroupInvocationCount);
        assertEquals("describeLogGroups invocation count",  1,      mock.describeLogGroupsInvocationCount);

        Log4JCapturingAppender.getInstance().assertLogEntry("creating.*log group.*foo", 0);
    }


    @Test
    public void testCreateLogGroupOperationAborted() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public CreateLogGroupResult createLogGroup(CreateLogGroupRequest request)
            {
                // this call won't succeed, but we'll store the group name so that describe works
                createLogGroupInvocationCount++;
                knownLogGroupNames.add(request.getLogGroupName());
                throw new OperationAbortedException("");
            }
        };
        AWSLogs client = mock.getInstance();

        LogGroup group = CloudWatchLogsUtil.createLogGroup(client, "bar", 100);

        assertEquals("returned LogGroup object",            "bar",      group.getLogGroupName());
        assertEquals("createLogGroup invocation count",     1,          mock.createLogGroupInvocationCount);
        assertEquals("describeLogGroups invocation count",  1,          mock.describeLogGroupsInvocationCount);

        Log4JCapturingAppender.getInstance().assertLogEntry("creating.*log group.*bar", 0);
    }


    @Test
    public void testCreateLogGroupTimeout() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                // will never complete
                describeLogGroupsInvocationCount++;
                return new DescribeLogGroupsResult().withLogGroups();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogGroup group = CloudWatchLogsUtil.createLogGroup(client, "bar", 200);
        long finish = System.currentTimeMillis();

        assertNull("did not return anything",                           group);
        assertEquals("createLogGroup invocation count",     1,          mock.createLogGroupInvocationCount);
        assertTrue("made multiple describes",                           mock.describeLogGroupsInvocationCount > 1);
        assertInRange("waited between describes",           150, 250,   (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("creating.*log group.*bar", 0);
        Log4JCapturingAppender.getInstance().assertLogEntry("timeout expired.*log group.*bar", 1);
    }


    @Test
    public void testCreateLogStream() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                if (describeLogStreamsInvocationCount++ == 0)
                    return new DescribeLogStreamsResult().withLogStreams();
                else
                    return super.describeLogStreams(request);
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogStream stream = CloudWatchLogsUtil.createLogStream(client, "foo", "bargle", 100);
        long finish = System.currentTimeMillis();

        assertEquals("returned LogStream object",           "bargle",   stream.getLogStreamName());
        assertEquals("describeLogGroups invocation count",  1,          mock.describeLogGroupsInvocationCount);
        assertEquals("describeLogStreams invocation count", 3,          mock.describeLogStreamsInvocationCount);
        assertEquals("createLogGroup invocation count",     0,          mock.createLogGroupInvocationCount);
        assertEquals("createLogStream invocation count",    1,          mock.createLogStreamInvocationCount);
        assertInRange("waited between describes",           20, 90,     (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("creating.*log stream.*foo.*bargle", 0);
    }


    @Test
    public void testCreateLogStreamAlreadyExists() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"));
        AWSLogs client = mock.getInstance();

        LogStream stream = CloudWatchLogsUtil.createLogStream(client, "foo", "argle", 100);

        assertEquals("returned LogStream object",           "argle",    stream.getLogStreamName());
        assertEquals("describeLogGroups invocation count",  1,          mock.describeLogGroupsInvocationCount);
        assertEquals("describeLogStreams invocation count", 1,          mock.describeLogStreamsInvocationCount);
        assertEquals("createLogGroup invocation count",     0,          mock.createLogGroupInvocationCount);
        assertEquals("createLogStream invocation count",    1,          mock.createLogStreamInvocationCount);

        Log4JCapturingAppender.getInstance().assertLogEntry("creating.*log stream.*foo.*argle", 0);
    }


    @Test
    public void testCreateLogStreamTimeout() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                describeLogStreamsInvocationCount++;
                return new DescribeLogStreamsResult().withLogStreams();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        LogStream stream = CloudWatchLogsUtil.createLogStream(client, "foo", "bargle", 200);
        long finish = System.currentTimeMillis();

        assertNull("did not return LogStream object",                   stream);
        assertEquals("describeLogGroups invocation count",  1,          mock.describeLogGroupsInvocationCount);
        assertTrue("multiple describeLogStreams calls",                 mock.describeLogStreamsInvocationCount > 2);
        assertEquals("createLogGroup invocation count",     0,          mock.createLogGroupInvocationCount);
        assertEquals("createLogStream invocation count",    1,          mock.createLogStreamInvocationCount);
        assertInRange("waited between describes",           150, 250,   (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("creating.*log stream.*foo.*bargle", 0);
        Log4JCapturingAppender.getInstance().assertLogEntry("timeout expired.*log stream.*bargle", 1);
    }


    @Test
    public void testCreateLogGroupAndStream() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"));
        AWSLogs client = mock.getInstance();

        LogStream stream = CloudWatchLogsUtil.createLogStream(client, "bar", "bargle", 100);

        assertEquals("returned LogStream object",           "bargle",   stream.getLogStreamName());
        assertEquals("describeLogGroups invocation count",  2,          mock.describeLogGroupsInvocationCount);
        assertEquals("describeLogStreams invocation count", 1,          mock.describeLogStreamsInvocationCount);
        assertEquals("createLogGroup invocation count",     1,          mock.createLogGroupInvocationCount);
        assertEquals("createLogStream invocation count",    1,          mock.createLogStreamInvocationCount);

        Log4JCapturingAppender.getInstance().assertLogEntry("creating.*log stream.*bar.*bargle", 0);
    }


    @Test
    public void testDeleteLogGroup() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
            {
                // delete will have removed known group name so we must simulate it
                if (describeLogGroupsInvocationCount++ < 2)
                {
                    LogGroup group = new LogGroup().withLogGroupName("foo");
                    return new DescribeLogGroupsResult().withLogGroups(group);
                }
                else
                    return new DescribeLogGroupsResult().withLogGroups();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        assertTrue(CloudWatchLogsUtil.deleteLogGroup(client, "foo", 200));
        long finish = System.currentTimeMillis();

        assertEquals("deleteLogGroup invocation count",     1,          mock.deleteLogGroupInvocationCount);
        assertEquals("describeLogGroups invocation count",  3,          mock.describeLogGroupsInvocationCount);
        assertInRange("waited between describes",           50, 150,    (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("deleting.*log group.*foo", 0);
    }


    @Test
    public void testDeleteNonExistantLogGroup() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"));
        AWSLogs client = mock.getInstance();

        assertTrue(CloudWatchLogsUtil.deleteLogGroup(client, "bar", 200));

        assertEquals("deleteLogGroup invocation count",     1,          mock.deleteLogGroupInvocationCount);
        assertEquals("describeLogGroups invocation count",  0,          mock.describeLogGroupsInvocationCount);

        Log4JCapturingAppender.getInstance().assertLogEntry("deleting.*log group.*bar", 0);
    }


    @Test
    public void testDeleteLogGroupOperatiionAborted() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DeleteLogGroupResult deleteLogGroup(DeleteLogGroupRequest request)
            {
                deleteLogGroupInvocationCount++;
                throw new OperationAbortedException("");
            }
        };
        AWSLogs client = mock.getInstance();

        assertTrue(CloudWatchLogsUtil.deleteLogGroup(client, "bar", 200));

        assertEquals("deleteLogGroup invocation count",     1,          mock.deleteLogGroupInvocationCount);
        assertEquals("describeLogGroups invocation count",  1,          mock.describeLogGroupsInvocationCount);

        Log4JCapturingAppender.getInstance().assertLogEntry("deleting.*log group.*bar", 0);
    }


    @Test
    public void testDeleteLogGroupTimeout() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DeleteLogGroupResult deleteLogGroup(DeleteLogGroupRequest request)
            {
                // we're going to make this happen by not actually deleting the group
                deleteLogGroupInvocationCount++;
                return new DeleteLogGroupResult();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        assertFalse(CloudWatchLogsUtil.deleteLogGroup(client, "foo", 200));
        long finish = System.currentTimeMillis();

        assertEquals("deleteLogGroup invocation count",     1,          mock.deleteLogGroupInvocationCount);
        assertEquals("describeLogGroups invocation count",  4,          mock.describeLogGroupsInvocationCount);
        assertInRange("waited between describes",           150, 250,   (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("deleting.*log group.*foo", 0);
        Log4JCapturingAppender.getInstance().assertLogEntry("timeout.*foo", 1);
    }


    @Test
    public void testDeleteLogStream() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {

            @Override
            public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
            {
                if (describeLogStreamsInvocationCount++ < 2)
                {
                    LogStream stream = new LogStream().withLogStreamName("foo").withLogStreamName("argle");
                    return new DescribeLogStreamsResult().withLogStreams(stream);
                }
                else
                {
                    return new DescribeLogStreamsResult().withLogStreams();
                }
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        assertTrue(CloudWatchLogsUtil.deleteLogStream(client, "foo", "argle", 200));
        long finish = System.currentTimeMillis();

        assertEquals("deleteLogStream invocation count",        1,          mock.deleteLogStreamInvocationCount);
        assertEquals("describeLogStreams invocation count",     3,          mock.describeLogStreamsInvocationCount);
        assertInRange("waited between describes",               50, 150,    (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("deleting.*log stream.*foo.*argle", 0);
    }


    @Test
    public void testDeleteNonExistantLogStream() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"));
        AWSLogs client = mock.getInstance();

        assertTrue(CloudWatchLogsUtil.deleteLogStream(client, "foo", "bargle", 200));

        assertEquals("deleteLogStream invocation count",        1,          mock.deleteLogStreamInvocationCount);
        assertEquals("describeLogStreams invocation count",     0,          mock.describeLogStreamsInvocationCount);

        Log4JCapturingAppender.getInstance().assertLogEntry("deleting.*log stream.*foo.*bargle", 0);
    }


    @Test
    public void testDeleteLogStreamOperatiionAborted() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DeleteLogStreamResult deleteLogStream(DeleteLogStreamRequest request)
            {
                deleteLogStreamInvocationCount++;
                throw new OperationAbortedException("");
            }
        };
        AWSLogs client = mock.getInstance();

        assertTrue(CloudWatchLogsUtil.deleteLogStream(client, "foo", "bargle", 200));

        assertEquals("deleteLogStream invocation count",        1,          mock.deleteLogStreamInvocationCount);
        assertEquals("describeLogStreams invocation count",     1,          mock.describeLogStreamsInvocationCount);

        Log4JCapturingAppender.getInstance().assertLogEntry("deleting.*log stream.*foo.*bargle", 0);
    }


    @Test
    public void testDeleteLogStreamTimeout() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient(Arrays.asList("foo"), Arrays.asList("argle"))
        {
            @Override
            public DeleteLogStreamResult deleteLogStream(DeleteLogStreamRequest request)
            {
                deleteLogStreamInvocationCount++;
                return new DeleteLogStreamResult();
            }
        };
        AWSLogs client = mock.getInstance();

        long start = System.currentTimeMillis();
        assertFalse(CloudWatchLogsUtil.deleteLogStream(client, "foo", "argle", 200));
        long finish = System.currentTimeMillis();

        assertEquals("deleteLogStream invocation count",        1,          mock.deleteLogStreamInvocationCount);
        assertEquals("describeLogStreams invocation count",     4,          mock.describeLogStreamsInvocationCount);
        assertInRange("waited between describes",               150, 250,   (finish - start));

        Log4JCapturingAppender.getInstance().assertLogEntry("deleting.*log stream.*foo.*argle", 0);
        Log4JCapturingAppender.getInstance().assertLogEntry("timeout.*foo.*argle", 1);
    }
}
