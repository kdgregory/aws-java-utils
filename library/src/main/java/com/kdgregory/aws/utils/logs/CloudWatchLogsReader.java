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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;


/**
 *  Reads messages from CloudWatch one or more CloudWatch Logs streams.
 */
public class CloudWatchLogsReader
{
    private AWSLogs client;
    private String logGroupName;
    private List<String> logStreamNames;

    private Long startTime;
    private Long endTime;


    /**
     *  Creates an instance that reads from one or more named streams in a single group.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logGroupName    The source log group.
     *  @param  logStreamName2  The source log streams.
     */
    public CloudWatchLogsReader(AWSLogs client, String logGroupName, String... logStreamNames)
    {
        this.client = client;
        this.logGroupName = logGroupName;
        this.logStreamNames = new ArrayList<String>(Arrays.asList(logStreamNames));
    }

//----------------------------------------------------------------------------
//  Configuration API
//----------------------------------------------------------------------------

    /**
     *  Restricts events to a specific time range. You can omit either start or
     *  end, to read from (respectively) the start or end of the stream.
     *
     *  @param  start       Java timestamp (millis since epoch) to start retrieving
     *                      messages, or <code>null</code> to start from first
     *                      message in stream.
     *  @param  finish      Java timestamp (millis since epoch) to stop retrieving
     *                      messages, or <code>null</code> to stop at end of stream.
     */
    public CloudWatchLogsReader withTimeRange(Long start, Long finish)
    {
        this.startTime = start;
        this.endTime = finish;
        return this;
    }


//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  Retrieves messages from the streams. All messages are combined into a
     *  single list, and are ordered with the earliest message first.
     *  <p>
     *  This is a "best effort" read: it starts reading at the configured start
     *  point, and continues to request records until the sequence token does
     *  not change between requests. This approach may miss records due to
     *  eventual consistency; it will definitely omit records that were written
     *  with older timestamps than those that are already read.
     *  <p>
     *  If the log group or log stream does not exist, it is ignored and the
     *  result will be an empty list.
     */
    public List<OutputLogEvent> retrieve()
    {
        List<OutputLogEvent> result = new ArrayList<OutputLogEvent>();

        for (String logStreamName : logStreamNames)
        {
            readFromStream(logGroupName, logStreamName, result);
        }

        Collections.sort(result, new OutputLogEventComparator());
        return result;
    }


    /**
     *  Retrieves messages from the streams, retrying until either the expected
     *  number of records have been read or the timeout expires.
     */
    public List<OutputLogEvent> retrieve(int expectedRecordCount, long timeoutInMillis)
    {
        List<OutputLogEvent> result = Collections.emptyList();

        long timeoutAt = System.currentTimeMillis() + timeoutInMillis;
        while (System.currentTimeMillis() < timeoutAt)
        {
            result = retrieve();
            if (result.size() == expectedRecordCount)
                return result;

            try
            {
                Thread.sleep(500);
            }
            catch (InterruptedException ex)
            {
                return result;
            }
        }
        return result;
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Reads messages from a single stream, storing messages in the provided
     *  list.
     */
    private void readFromStream(String groupName, String streamName, List<OutputLogEvent> output)
    {
        GetLogEventsRequest request = new GetLogEventsRequest()
                                      .withLogGroupName(groupName)
                                      .withLogStreamName(streamName);
        if (startTime != null)
            request.setStartTime(startTime);

        if (endTime != null)
            request.setEndTime(endTime);

        String prevToken = "";
        String nextToken = "";
        do
        {
            try
            {
                GetLogEventsResult response = client.getLogEvents(request);
                output.addAll(response.getEvents());
                prevToken = nextToken;
                nextToken = response.getNextForwardToken();
                request.setNextToken(nextToken);
            }
            catch (ResourceNotFoundException ex)
            {
                return;
            }
        }
        while (! prevToken.equals(nextToken));
    }


    /**
     *  A comparator to sort log events by timestmap.
     */
    private static class OutputLogEventComparator
    implements Comparator<OutputLogEvent>
    {
        @Override
        public int compare(OutputLogEvent o1, OutputLogEvent o2)
        {
            return o1.getTimestamp().compareTo(o2.getTimestamp());
        }
    }

}
