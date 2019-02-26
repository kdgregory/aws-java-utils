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

package com.kdgregory.aws.utils.testhelpers.mocks;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import net.sf.kdgcommons.lang.StringUtil;
import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

/**
 *  A mock client that will knows about a predefined set of groups and streams.
 */
public class MockAWSLogsClient
extends SelfMock<AWSLogs>
{
    // public variables will be inspected by tests, protected variables may
    // be changed by subclasses but are not expected to be used by tests

    public int createLogGroupInvocationCount = 0;
    public int createLogStreamInvocationCount = 0;
    public int deleteLogGroupInvocationCount = 0;
    public int deleteLogStreamInvocationCount = 0;
    public int describeLogGroupsInvocationCount = 0;
    public int describeLogStreamsInvocationCount = 0;
    public int putLogEventsInvocationCount = 0;

    protected String uploadSequenceToken = UUID.randomUUID().toString();

    public PutLogEventsRequest lastBatch;
    public List<InputLogEvent> allMessages = new ArrayList<InputLogEvent>();

    private Map<String,TreeSet<String>> groupsAndStreams = new TreeMap<String,TreeSet<String>>();
    private int pageSize = Integer.MAX_VALUE;


    /**
     *  Basic constructor: must call one or more of the configuration methods
     *  for this to be useful.
     */
    public MockAWSLogsClient()
    {
        super(AWSLogs.class);
    }


    /**
     *  Convenience constructor, for a single log group and stream.
     */
    public MockAWSLogsClient(String knownLogGroupName, String knownLogStreamName)
    {
        this();
        addStream(knownLogGroupName, knownLogStreamName);
    }

//----------------------------------------------------------------------------
//  Optional configuration
//----------------------------------------------------------------------------

    /**
     *  Adds a group and its list of streams (which may be empty).
     */
    public MockAWSLogsClient withGroupAndStreams(String groupName, String... streamNames)
    {
        addGroup(groupName);
        for (String streamName : streamNames)
        {
            addStream(groupName, streamName);
        }
        return this;
    }
        
        
    /**
     *  Sets the page size for paginated describes.
     */
    public MockAWSLogsClient withPageSize(int value)
    {
        this.pageSize = value;
        return this;
    }

//----------------------------------------------------------------------------
//  Internals
//---------------------------------------------------------------------------

    protected void addGroup(String groupName)
    {
        if (groupsAndStreams.containsKey(groupName))
            return;

        groupsAndStreams.put(groupName, new TreeSet<String>());
    }


    protected void addStream(String groupName, String streamName)
    {
        addGroup(groupName);
        groupsAndStreams.get(groupName).add(streamName);
    }

//----------------------------------------------------------------------------
//  Mock implementations
//----------------------------------------------------------------------------

    public CreateLogGroupResult createLogGroup(CreateLogGroupRequest request)
    {
        createLogGroupInvocationCount++;
        String groupName = request.getLogGroupName();
        if (groupsAndStreams.containsKey(groupName))
            throw new ResourceAlreadyExistsException("resource exists: " + groupName);

        addGroup(groupName);
        return new CreateLogGroupResult();
    }


    public CreateLogStreamResult createLogStream(CreateLogStreamRequest request)
    {
        createLogStreamInvocationCount++;

        String groupName = request.getLogGroupName();
        if (! groupsAndStreams.containsKey(groupName))
            throw new ResourceNotFoundException("group does not exist: " + groupName);

        String streamName = request.getLogStreamName();
        if (groupsAndStreams.get(groupName).contains(streamName))
            throw new ResourceAlreadyExistsException("stream already exists: " + streamName);

        addStream(groupName, streamName);
        return new CreateLogStreamResult();
    }


    public DeleteLogGroupResult deleteLogGroup(DeleteLogGroupRequest request)
    {
        deleteLogGroupInvocationCount++;
        String groupName = request.getLogGroupName();
        if (! groupsAndStreams.containsKey(groupName))
        {
            throw new ResourceNotFoundException("no such group: " + groupName);
        }
        groupsAndStreams.remove(groupName);
        return new DeleteLogGroupResult();
    }


    public DeleteLogStreamResult deleteLogStream(DeleteLogStreamRequest request)
    {
        deleteLogStreamInvocationCount++;

        String groupName = request.getLogGroupName();
        if (! groupsAndStreams.containsKey(groupName))
        {
            throw new ResourceNotFoundException("no such group: " + groupName);
        }

        String streamName = request.getLogStreamName();
        if (! groupsAndStreams.get(groupName).contains(streamName))
        {
            throw new ResourceNotFoundException("no such stream: " + streamName);
        }
        groupsAndStreams.get(groupName).remove(streamName);
        return new DeleteLogStreamResult();
    }


    public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
    {
        describeLogGroupsInvocationCount++;

        List<String> groupNames = new ArrayList<String>(groupsAndStreams.keySet());

        int startOffset = StringUtil.isEmpty(request.getNextToken())
                        ? 0
                        : Integer.parseInt(request.getNextToken());
        int endOffset = Math.min(groupNames.size(), startOffset + pageSize);
        String nextToken = endOffset == groupNames.size()
                         ? null
                         : String.valueOf(endOffset);

        List<LogGroup> groups = new ArrayList<LogGroup>();
        for (String name : groupNames.subList(startOffset, endOffset))
        {
            boolean include = StringUtil.isEmpty(request.getLogGroupNamePrefix())
                           || name.startsWith(request.getLogGroupNamePrefix());
            if (include)
                groups.add(new LogGroup().withLogGroupName(name));
        }

        return new DescribeLogGroupsResult().withLogGroups(groups).withNextToken(nextToken);
    }


    public DescribeLogStreamsResult describeLogStreams(DescribeLogStreamsRequest request)
    {
        describeLogStreamsInvocationCount++;

        String groupName = request.getLogGroupName();
        if (! groupsAndStreams.containsKey(groupName))
            throw new ResourceNotFoundException("no such log group: " + groupName);

        List<String> streamNames = new ArrayList<String>(groupsAndStreams.get(groupName));

        int startOffset = StringUtil.isEmpty(request.getNextToken())
                        ? 0
                        : Integer.parseInt(request.getNextToken());
        int endOffset = Math.min(streamNames.size(), startOffset + pageSize);
        String nextToken = endOffset == streamNames.size()
                         ? null
                         : String.valueOf(endOffset);

        List<LogStream> streams = new ArrayList<LogStream>();
        for (String name : streamNames.subList(startOffset, endOffset))
        {
            boolean include = StringUtil.isEmpty(request.getLogStreamNamePrefix())
                           || name.startsWith(request.getLogStreamNamePrefix());
            if (include)
            {
                LogStream stream = new LogStream().withLogStreamName(name)
                                   .withUploadSequenceToken(uploadSequenceToken);
                streams.add(stream);
            }
        }

        return new DescribeLogStreamsResult().withLogStreams(streams).withNextToken(nextToken);
    }


    public PutLogEventsResult putLogEvents(PutLogEventsRequest request)
    {
        putLogEventsInvocationCount++;
        lastBatch = request;
        allMessages.addAll(request.getLogEvents());

        uploadSequenceToken = UUID.randomUUID().toString();
        return new PutLogEventsResult().withNextSequenceToken(uploadSequenceToken);
    }
}