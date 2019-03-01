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

import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazonaws.services.logs.model.*;

import com.kdgregory.aws.utils.testhelpers.mocks.MockAWSLogsClient;


public class TestCloudWatchLogsReader
{
//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    private void assertEvent(List<OutputLogEvent> events, int index, long expectedTimestamp, String expectedMessage)
    {
        assertEquals("event " + index + ", timestamp",  expectedTimestamp,  events.get(index).getTimestamp().longValue());
        assertEquals("event " + index + ", message",    expectedMessage,    events.get(index).getMessage());
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testBasicOperation() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient("foo", "bar")
                                 .withMessage(10, "first")
                                 .withMessage(20, "second")
                                 .withMessage(30, "third");

        CloudWatchLogsReader reader = new CloudWatchLogsReader(mock.getInstance(), "foo", "bar");
        List<OutputLogEvent> events = reader.retrieve();

        // will always call getLogEvents one time more than necessary, to determine end of stream

        assertEquals("calls to getLogEvents",   2,  mock.getLogEventsInvocationCount);
        assertEquals("number of events",        3,  events.size());

        assertEvent(events, 0, 10, "first");
        assertEvent(events, 1, 20, "second");
        assertEvent(events, 2, 30, "third");
    }


    @Test
    public void testPaginatedRetrieve() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient("foo", "bar")
                                 .withMessage(10, "first")
                                 .withMessage(20, "second")
                                 .withMessage(30, "third")
                                 .withPageSize(1);

        CloudWatchLogsReader reader = new CloudWatchLogsReader(mock.getInstance(), "foo", "bar");
        List<OutputLogEvent> events = reader.retrieve();

        assertEquals("calls to getLogEvents",   4,  mock.getLogEventsInvocationCount);
        assertEquals("number of events",        3,  events.size());

        assertEvent(events, 0, 10, "first");
        assertEvent(events, 1, 20, "second");
        assertEvent(events, 2, 30, "third");
    }


    @Test
    public void testTimeRange() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient("foo", "bar")
                                 .withMessage(10, "first")
                                 .withMessage(20, "second")
                                 .withMessage(30, "third");

        CloudWatchLogsReader reader = new CloudWatchLogsReader(mock.getInstance(), "foo", "bar")
                                      .withTimeRange(15L, 25L);
        List<OutputLogEvent> events1 = reader.retrieve();

        assertEquals("calls to getLogEvents",   2,  mock.getLogEventsInvocationCount);
        assertEquals("number of events",        1,  events1.size());

        assertEvent(events1, 0, 20, "second");

        // I worry that this is testing the mock rather than the class, but let's cover all variants

        List<OutputLogEvent> events2 = reader.withTimeRange(null, 25L).retrieve();
        assertEquals("number of events", 2, events2.size());
        assertEvent(events2, 0, 10, "first");
        assertEvent(events2, 1, 20, "second");

        List<OutputLogEvent> events3 = reader.withTimeRange(15L, null).retrieve();
        assertEquals("number of events", 2, events3.size());
        assertEvent(events3, 0, 20, "second");
        assertEvent(events3, 1, 30, "third");
    }


    @Test
    public void testNoMessages() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient("foo", "bar");

        CloudWatchLogsReader reader = new CloudWatchLogsReader(mock.getInstance(), "foo", "bar");
        List<OutputLogEvent> events = reader.retrieve();

        assertEquals("calls to getLogEvents",   2,  mock.getLogEventsInvocationCount);
        assertEquals("number of events",        0,  events.size());
    }


    @Test
    public void testMissingLogGroup() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient("foo", "bar");

        CloudWatchLogsReader reader = new CloudWatchLogsReader(mock.getInstance(), "zippy", "bar");
        List<OutputLogEvent> events = reader.retrieve();

        assertEquals("calls to getLogEvents",   1,  mock.getLogEventsInvocationCount);
        assertEquals("number of events",        0,  events.size());
    }


    @Test
    public void testMissingLogStream() throws Exception
    {
        MockAWSLogsClient mock = new MockAWSLogsClient("foo", "bar");

        CloudWatchLogsReader reader = new CloudWatchLogsReader(mock.getInstance(), "foo", "bargle");
        List<OutputLogEvent> events = reader.retrieve();

        assertEquals("calls to getLogEvents",   1,  mock.getLogEventsInvocationCount);
        assertEquals("number of events",        0,  events.size());
    }
}
