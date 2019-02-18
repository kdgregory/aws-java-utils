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

    protected ArrayList<String> knownLogGroupNames = new ArrayList<String>();
    protected ArrayList<String> knownLogStreamNames = new ArrayList<String>();
    protected int pageSize = Integer.MAX_VALUE;


    /**
     *  Basic constructor.
     */
    public MockAWSLogsClient(List<String> knownLogGroupNames, List<String> knownLogStreamNames)
    {
        super(AWSLogs.class);
        this.knownLogGroupNames.addAll(knownLogGroupNames);
        this.knownLogStreamNames.addAll(knownLogStreamNames);
    }


    /**
     *  Constructor for paginated describes. Note that the page size applies to the
     *  raw list of groups and streams, not the filtered list.
     */
    public MockAWSLogsClient(List<String> knownLogGroupNames, List<String> knownLogStreamNames, int pageSize)
    {
        this(knownLogGroupNames, knownLogStreamNames);
        this.pageSize = pageSize;
    }

//----------------------------------------------------------------------------
//  Mock implementations
//----------------------------------------------------------------------------

    public CreateLogGroupResult createLogGroup(CreateLogGroupRequest request)
    {
        createLogGroupInvocationCount++;
        String groupName = request.getLogGroupName();
        if (knownLogGroupNames.contains(groupName))
            throw new ResourceAlreadyExistsException("resource exists: " + groupName);

        knownLogGroupNames.add(groupName);
        return new CreateLogGroupResult();
    }


    public CreateLogStreamResult createLogStream(CreateLogStreamRequest request)
    {
        createLogStreamInvocationCount++;

        String groupName = request.getLogGroupName();
        if (! knownLogGroupNames.contains(groupName))
            throw new ResourceNotFoundException("group does not exist: " + groupName);

        String streamName = request.getLogStreamName();
        if (knownLogStreamNames.contains(streamName))
            throw new ResourceAlreadyExistsException("stream already exists: " + streamName);

        knownLogStreamNames.add(streamName);
        return new CreateLogStreamResult();
    }


    public DeleteLogGroupResult deleteLogGroup(DeleteLogGroupRequest request)
    {
        deleteLogGroupInvocationCount++;
        String groupName = request.getLogGroupName();
        if (! knownLogGroupNames.contains(groupName))
        {
            throw new ResourceNotFoundException("no such group: " + groupName);
        }
        knownLogGroupNames.remove(groupName);
        return new DeleteLogGroupResult();
    }


    public DeleteLogStreamResult deleteLogStream(DeleteLogStreamRequest request)
    {
        deleteLogStreamInvocationCount++;
        String streamName = request.getLogStreamName();
        if (! knownLogStreamNames.contains(streamName))
        {
            throw new ResourceNotFoundException("no such stream: " + streamName);
        }
        knownLogStreamNames.remove(streamName);
        return new DeleteLogStreamResult();
    }


    public DescribeLogGroupsResult describeLogGroups(DescribeLogGroupsRequest request)
    {
        describeLogGroupsInvocationCount++;

        int startOffset = StringUtil.isEmpty(request.getNextToken())
                        ? 0
                        : Integer.parseInt(request.getNextToken());
        int endOffset = Math.min(knownLogGroupNames.size(), startOffset + pageSize);
        String nextToken = endOffset == knownLogGroupNames.size()
                         ? null
                         : String.valueOf(endOffset);

        List<LogGroup> groups = new ArrayList<LogGroup>();
        for (String name : knownLogGroupNames.subList(startOffset, endOffset))
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

        if (! knownLogGroupNames.contains(request.getLogGroupName()))
            throw new ResourceNotFoundException("no such log group");

        int startOffset = StringUtil.isEmpty(request.getNextToken())
                        ? 0
                        : Integer.parseInt(request.getNextToken());
        int endOffset = Math.min(knownLogStreamNames.size(), startOffset + pageSize);
        String nextToken = endOffset == knownLogStreamNames.size()
                         ? null
                         : String.valueOf(endOffset);

        List<LogStream> streams = new ArrayList<LogStream>();
        for (String name : knownLogStreamNames.subList(startOffset, endOffset))
        {
            boolean include = StringUtil.isEmpty(request.getLogStreamNamePrefix())
                           || name.startsWith(request.getLogStreamNamePrefix());
            if (include)
                streams.add(new LogStream().withLogStreamName(name));
        }

        return new DescribeLogStreamsResult().withLogStreams(streams).withNextToken(nextToken);
    }
}