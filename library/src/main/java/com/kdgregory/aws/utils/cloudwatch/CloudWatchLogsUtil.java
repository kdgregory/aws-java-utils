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

package com.kdgregory.aws.utils.cloudwatch;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

/**
 *  Provides static utility methods for working with CloudWatch Logs.
 */
public class CloudWatchLogsUtil
{
    private static Log logger = LogFactory.getLog(CloudWatchLogsUtil.class);
    
    /**
     *  The milliseconds to wait between describes when creating or deleting a resource.
     *  This is exposed for testing.
     */
    public final static long RESOURCE_TRANSITION_DESCRIBE_INTERVAL = 50;


    /**
     *  Creates a log group, waiting until it is describable, a timeout elapses, or the
     *  thread is interrupted. No-op if the group exists at time of call.
     *
     *  @param  client          The service client.
     *  @param  groupName       The name of the group to create.
     *  @param  timeout         Milliseconds to wait for group to become available. Will
     *                          return false if unable to describe group before timeout,
     *                          although it may be describable afterward.
     *
     *  @return A retrieved <code>LogGroup</code> object if the group was successfully created
     *          or already exists, <code>null</code> if the timeout expired or the thread was
     *          interrupted before group was verified.
     */
    public static LogGroup createLogGroup(AWSLogs client, String groupName, long timeout)
    {
        logger.debug("creating CloudWatch log group: " + groupName);

        try
        {
            CreateLogGroupRequest request = new CreateLogGroupRequest().withLogGroupName(groupName);
            client.createLogGroup(request);
        }
        catch (ResourceAlreadyExistsException ex)
        {
            // fall through so that we can return the LogGroup object
        }
        catch (OperationAbortedException ex)
        {
            // someone else is trying to create, fall through and wait until they're done
        }

        return waitUntilCreated(client, groupName, timeout, RESOURCE_TRANSITION_DESCRIBE_INTERVAL);
    }


    /**
     *  Creates a log stream, waiting until it is describable, a timeout elapses, or the
     *  thread is interrupted. Also creates the specified log group if it does not exist.
     *  No-op if the stream exists at time of call.
     *
     *  @param  client          The service client.
     *  @param  groupName       The name of the group containing the stream.
     *  @param  streamName      The name of the stream.
     *  @param  timeout         Milliseconds to wait for created resources to be available.
     *                          Note that the same timeout is applied to group and stream,
     *                          so effective timeout is double the passed value.
     *
     *  @return A retrieved <code>LogStream</code> object if the stream was successfully created
     *          or already exists, <code>null</code> if the timeout expired or the thread was
     *          interrupted before stream was verified.
     */
    public static LogStream createLogStream(AWSLogs client, String groupName, String streamName, long timeout)
    {
        logger.debug("creating CloudWatch log stream: " + groupName + "/" + streamName);

        LogGroup group = describeLogGroup(client, groupName);
        if (group == null)
        {
            createLogGroup(client, groupName, timeout);
        }

        try
        {
            CreateLogStreamRequest request = new CreateLogStreamRequest()
                                             .withLogGroupName(groupName)
                                             .withLogStreamName(streamName);
            client.createLogStream(request);
        }
        catch (ResourceAlreadyExistsException ex)
        {
            // fall through so that we can retrieve group name
        }

        return waitUntilCreated(client, groupName, streamName, timeout, RESOURCE_TRANSITION_DESCRIBE_INTERVAL);
    }


    /**
     *  Deletes a log group, waiting for it to go away, a timeout elapses, or the
     *  calling thread is interrupted.
     *
     *  @param  client          The service client.
     *  @param  groupName       The name of the group.
     *  @param  timeout         Number of milliseconds to wait for confirmation that the
     *                          group has been deleted.
     *
     *  @return <code>true</code> if the group was confirmed deleted, <code>false</code>
     *          if not (timeout or interrupt).
     */
    public static boolean deleteLogGroup(AWSLogs client, String groupName, long timeout)
    {
        logger.debug("deleting CloudWatch log group: " + groupName);

        try
        {
            DeleteLogGroupRequest request = new DeleteLogGroupRequest().withLogGroupName(groupName);
            client.deleteLogGroup(request);
        }
        catch (ResourceNotFoundException ex)
        {
            return true;
        }
        catch (OperationAbortedException ex)
        {
            // fall through to verification loop
        }

        long timeoutAt = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < timeoutAt)
        {
            if (describeLogGroup(client, groupName) == null)
                return true;

            try
            {
                Thread.sleep(RESOURCE_TRANSITION_DESCRIBE_INTERVAL);
            }
            catch (InterruptedException ex)
            {
                return false;
            }
        }

        logger.warn("timeout waiting for deleted CloudWatch log group: " + groupName);
        return false;
    }


    /**
     *  Deletes a log stream, waiting for it to go away, a timeout elapses, or the
     *  calling thread is interrupted.
     *
     *  @param  client          The service client.
     *  @param  groupName       The name of the group.
     *  @param  streamName      The name of the stream.
     *  @param  timeout         Number of milliseconds to wait for confirmation that the
     *                          stream has been deleted.
     *
     *  @return <code>true</code> if the stream was confirmed deleted, <code>false</code>
     *          if not (timeout or interrupt).
     */
    public static boolean deleteLogStream(AWSLogs client, String groupName, String streamName, long timeout)
    {
        logger.debug("deleting CloudWatch log stream: " + groupName + "/" + streamName);

        try
        {
            DeleteLogStreamRequest request = new DeleteLogStreamRequest()
                                             .withLogGroupName(groupName)
                                             .withLogStreamName(streamName);
            client.deleteLogStream(request);
        }
        catch (ResourceNotFoundException ex)
        {
            return true;
        }
        catch (OperationAbortedException ex)
        {
            // fall through to verification loop
        }

        long timeoutAt = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < timeoutAt)
        {
            if (describeLogStream(client, groupName, streamName) == null)
                return true;

            try
            {
                Thread.sleep(RESOURCE_TRANSITION_DESCRIBE_INTERVAL);
            }
            catch (InterruptedException ex)
            {
                return false;
            }
        }

        logger.debug("timeout waiting for deleted CloudWatch log stream: " + groupName + "/" + streamName);
        return false;
    }


    /**
     *  Calls <code>AWSLogs.describeLogGroups()</code>, handling pagination.
     *
     *  @param  client          The service client.
     *  @param  prefix          The group name prefix. Pass null or an empty string
     *                          to retrieve all groups.
     *
     *  @return A list of the groups matching that prefix.
     */
    public static List<LogGroup> describeLogGroups(AWSLogs client, String prefix)
    {
        List<LogGroup> result = new ArrayList<LogGroup>();

        DescribeLogGroupsRequest request = new DescribeLogGroupsRequest();
        if ((prefix != null) && (prefix.length() > 0))
            request.setLogGroupNamePrefix(prefix);

        DescribeLogGroupsResult response = null;
        do
        {
            response = client.describeLogGroups(request);
            result.addAll(response.getLogGroups());
            request.setNextToken(response.getNextToken());
        } while ((response.getNextToken() != null) && (response.getNextToken().length() > 0));

        return result;
    }


    /**
     *  Calls <code>describeLogGroups()</code>, looking for a single group in the result.
     *
     *  @param  client          The service client.
     *  @param  groupName       The group name. Must not be null.
     *
     *  @return The log group, <code>null</code> if a group with that exact name is does
     *          not exist.
     */
    public static LogGroup describeLogGroup(AWSLogs client, String groupName)
    {
        for (LogGroup group : describeLogGroups(client, groupName))
        {
            if (group.getLogGroupName().equals(groupName))
                return group;
        }
        return null;
    }


    /**
     *  Calls <code>AWSLogs.describeLogStreams()</code>, handling pagination.
     *
     *  @param  client          The service client.
     *  @param  groupName       The name of the log group containing the stream.
     *  @param  prefix          The stream name prefix. Pass null or an empty string
     *                          to retrieve all strems in the group.
     *
     *  @return A list of the streams matching that prefix in the specified group.
     *          If the group does not exist this will be empty.
     */
    public static List<LogStream> describeLogStreams(AWSLogs client, String groupName, String prefix)
    {
        List<LogStream> result = new ArrayList<LogStream>();

        DescribeLogStreamsRequest request = new DescribeLogStreamsRequest(groupName);
        if ((prefix != null) && (prefix.length() > 0))
            request.setLogStreamNamePrefix(prefix);

        try
        {
            DescribeLogStreamsResult response = null;
            do
            {
                response = client.describeLogStreams(request);
                result.addAll(response.getLogStreams());
                request.setNextToken(response.getNextToken());
            } while ((response.getNextToken() != null) && (response.getNextToken().length() > 0));
        }
        catch (ResourceNotFoundException ex)
        {
            // result will be empty, so just drop through
        }

        return result;
    }


    /**
     *  Calls <code>describeLogStreams()</code>, looking for a single group in the result.
     *
     *  @param  client          The service client.
     *  @param  groupName       The name of the log group containing the stream.
     *  @param  streamName      The name of the stream. Must not be null.
     *
     *  @return The log stream, <code>null</code> if either the requested group or stream
     *          does not exist.
     */
    public static LogStream describeLogStream(AWSLogs client, String groupName, String streamName)
    {
        for (LogStream stream : describeLogStreams(client, groupName, streamName))
        {
            if (stream.getLogStreamName().equals(streamName))
                return stream;
        }
        return null;
    }


    /**
     *  Waits until a log group has been created, a timeout elapses, or the thread
     *  was interrupted.
     *
     *  @param  client          The service client.
     *  @param  groupName       The name of the group.
     *  @param  timeout         The number of milliseconds to attempt to retrieve
     *                          the group's information before giving up.
     *  @param  retryInterval   The number of milliseconds to wait between attempts
     *                          to retrieve the group information. This is used to
     *                          avoid throttling; 250 is generally a good number.
     *
     *  @return The group description, <code>null</code> if unable to retrieve the
     *          group before the timeout expired.
     */
    public static LogGroup waitUntilCreated(AWSLogs client, String groupName, long timeout, long retryInterval)
    {
        long timeoutAt = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < timeoutAt)
        {
            LogGroup group = describeLogGroup(client, groupName);
            if (group != null)
                return group;

            try
            {
                Thread.sleep(retryInterval);
            }
            catch (InterruptedException e)
            {
                return null;
            }
        }

        logger.warn("timeout expired waiting for CloudWatch log group creation: " + groupName);
        return null;
    }


    /**
     *  Waits until a log stream (and its containing group) has been created, a timeout
     *  elapses, or the thread was interrupted.
     *
     *  @param  client          The service client.
     *  @param  groupName       The name of the group.
     *  @param  groupName       The name of the stream.
     *  @param  timeout         The number of milliseconds to attempt to retrieve
     *                          the group's information before giving up.
     *  @param  retryInterval   The number of milliseconds to wait between attempts
     *                          to retrieve the group information. This is used to
     *                          avoid throttling; 250 is generally a good number.
     *
     *  @return The group description, <code>null</code> if unable to retrieve the
     *          group before the timeout expired.
     */
    public static LogStream waitUntilCreated(AWSLogs client, String groupName, String streamName, long timeout, long retryInterval)
    {
        long timeoutAt = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < timeoutAt)
        {
            try
            {
                LogStream stream = describeLogStream(client, groupName, streamName);
                if (stream != null)
                    return stream;
            }
            catch (ResourceNotFoundException ex)
            {
                // this indicates that the group has not yet been created
            }

            try
            {
                Thread.sleep(retryInterval);
            }
            catch (InterruptedException e)
            {
                return null;
            }
        }

        logger.warn("timeout expired waiting for CloudWatch log stream creation: " + groupName + "/" + streamName);
        return null;
    }
}
