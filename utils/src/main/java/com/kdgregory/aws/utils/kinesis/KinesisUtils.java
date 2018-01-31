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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;

import com.kdgregory.aws.utils.CommonUtils;


/**
 *  General utility methods for Kinesis. These methods perform retries with
 *  exponential backoff when throttled, and handle the case where AWS requires
 *  multiple calls to retrieve all information.
 */
public class KinesisUtils
{
    /**
     *  Extracts the status from a stream description and converts it from a string
     *  to a <code>StreamStatus</code> value. Returns null if the description is null
     *  or the status is unconvertable (which should never happen in the real world).
     */
    public static StreamStatus getStatus(StreamDescription description)
    {
        if (description == null) return null;

        try
        {
            return StreamStatus.valueOf(description.getStreamStatus());
        }
        catch (IllegalArgumentException ex)
        {
            // this should never happen with an AWS-constructed description but let's be sure
            return null;
        }
    }


    /**
     *  Executes a <code>DescribeStream</code> request, returning the stream
     *  description from the response. Will return null if the stream does not
     *  exist, cannot be read within the specified timeout, or if the thread
     *  is interrupted.
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  request         The request that's passed to the client.
     *  @param  timeout         The total number of milliseconds to attempt retries
     *                          due to throttling. The initial retry is 100 millis,
     *                          and this doubles for each retry.
     */
    public static StreamDescription describeStream(AmazonKinesis client, DescribeStreamRequest request, long timeout)
    {
        long currentSleep = 100;
        long timeoutAt = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < timeoutAt)
        {
            try
            {
                return client.describeStream(request).getStreamDescription();
            }
            catch (ResourceNotFoundException ex)
            {
                return null;
            }
            catch (LimitExceededException ex)
            {
                if (CommonUtils.sleepQuietly(currentSleep)) return null;
                currentSleep *= 2;
            }
        }
        return null;
    }


    /**
     *  A wrapper around <code>DescribeStream</code> that retrieves all shard
     *  descriptions and retries if throttled. Returns null if the stream does
     *  not exist, the calling thread was interrupted, or we could not retrieve
     *  all shards within the timeout (this method does not return partial
     *  results).
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  streamName      Identifies the stream to describe.
     *  @param  timeout         The total number of milliseconds to attempt retries due
     *                          to throttling.
     */
    public static List<Shard> describeShards(AmazonKinesis client, String streamName, long timeout)
    {
        List<Shard> result = new ArrayList<Shard>();
        long timeoutAt = System.currentTimeMillis() + timeout;
        String lastShardId = null;
        do
        {
            long currentTimeout = timeoutAt - System.currentTimeMillis();
            DescribeStreamRequest request = new DescribeStreamRequest().withStreamName(streamName);
            if (lastShardId != null) request.setExclusiveStartShardId(lastShardId);
            StreamDescription description = describeStream(client, request, currentTimeout);
            if (description == null) return null;
            List<Shard> shards = description.getShards();
            result.addAll(shards);
            lastShardId = description.getHasMoreShards()
                        ? shards.get(shards.size() - 1).getShardId()
                        : null;
        } while (lastShardId != null);
        return result;
    }


    /**
     *  Repeatedly calls {@link #describeStream} (with a pause between calls) until
     *  the stream's status is the desired value, the calling thrad is interrupted,
     *  or the timeout expires. Returns the last retrieved status value (which may
     *  be null even if the actual status is not, due to timeout or interruption).
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  streamName      Identifies the stream to describe.
     *  @param  desiredStatus   The desired stream status.
     *  @param  timeout         The total number of milliseconds to attempt retries due
     *                          to throttling.
     */
    public static StreamStatus waitForStatus(AmazonKinesis client, String streamName, StreamStatus desiredStatus, long timeout)
    {
        StreamStatus lastStatus = null;
        long timeoutAt = System.currentTimeMillis() + timeout;
        long remainingTimeout = 0;
        DescribeStreamRequest request = new DescribeStreamRequest().withStreamName(streamName);

        while ((remainingTimeout = timeoutAt - System.currentTimeMillis()) > 0)
        {
            StreamDescription description = describeStream(client, request, remainingTimeout);
            lastStatus = getStatus(description);
            if (lastStatus == desiredStatus) break;

            // sleep to avoid throttling
            CommonUtils.sleepQuietly(100);
        }
        return lastStatus;
    }


    /**
     *  Creates a stream and waits for it to become available.
     *
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  numShards   The number of shards to create.
     *  @param  timeout     The number of milliseconds to wait for the stream to become available (this
     *                      also controls the number of attempts that we'll make to create the stream
     *                      if throttled).
     *
     *  @return The final status retrieved. If this is <code>null</code> it means that thes stream
     *          was not created. Caller should only use the stream if it's <code>ACTIVE</code>/
     *
     *  @throws IllegalArgumentException if thrown by the underlying calls (ie, this is not caught).
     */
    public static StreamStatus createStream(AmazonKinesis client, String streamName, int numShards, long timeout)
    {
        long currentSleep = 100;
        long abortAt = System.currentTimeMillis() + timeout;
        while (System.currentTimeMillis() < abortAt)
        {
            CreateStreamRequest request = new CreateStreamRequest()
                                              .withStreamName(streamName)
                                              .withShardCount(numShards);
            try
            {
                client.createStream(request);
                break;
            }
            catch (LimitExceededException ex)
            {
                if (CommonUtils.sleepQuietly(currentSleep)) return null;
                currentSleep *= 2;
            }
            catch (ResourceInUseException ex)
            {
                // the documentation isn't very clear on this exception; it's thrown when
                // the stream already exists
                break;
            }
        }

        return waitForStatus(client, streamName, StreamStatus.ACTIVE, (abortAt - System.currentTimeMillis()));
    }


    /**
     *  Deletes a stream, retrying if throttled. Does not wait for the stream to go
     *  away; call {@link #waitForStatus} to do that.
     *
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  timeout     The maximum amount of time (millis) that we'll spend trying to
     *                      delete the stream.
     *
     *  @return true if the <code>deleteStream</code> request completed successfully,
     *          <code>false</code> if it failed for any reason (timed out or the stream
     *          did not exist).
     */
    public static boolean deleteStream(AmazonKinesis client, String streamName, long timeout)
    {
        long timeoutAt = System.currentTimeMillis() + timeout;
        long currentSleep = 100;
        while (System.currentTimeMillis() < timeoutAt)
        {
            try
            {
                client.deleteStream(new DeleteStreamRequest().withStreamName(streamName));
                return true;
            }
            catch (LimitExceededException ex)
            {
                CommonUtils.sleepQuietly(currentSleep);
                currentSleep *= 2;
            }
            catch (ResourceNotFoundException ex)
            {
                return false;
            }
        }
        return false;
    }


    /**
     *  Updates a stream's retention period and waits for it to become available again. This
     *  function looks at the current retention period and chooses either the "increase" or
     *  "decrease" operation. Calling with the same retention period is a no-op (other than
     *  calls to retrieve the stream status).
     *
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  retention   The new retention period (hours)
     *  @param  timeout     The maximum amount of time (millis) that we'll spend trying to
     *                      update the stream or wait for it to become available.
     *
     *  @return The stream status. After successful update this will be ACTIVE. It will be
     *          <code>UPDATING</code> if the update operation completed but the stream was
     *          still updating when the timeout expired. If the update call timed out or
     *          the stream does not exist, will return <code>null</code>.
     *
     *  @throws IllegalArgumentException if thrown by underlying call. The AWS docs don't
     *          give specific reasons for this exception, but one cause is specifying a
     *          retention period outside the allowed 24..168 hours.
     */
    public static StreamStatus updateRetentionPeriod(AmazonKinesis client, String streamName, int retention, long timeout)
    {
        long timeoutAt = System.currentTimeMillis() + timeout;

        // this is certainly not the most efficient way to do things, but the code is simple
        // and nobody is going to be calling this method in a tight loop

        int currentSleep = 100;
        while (System.currentTimeMillis() < timeoutAt)
        {
            StreamStatus initialStatus = waitForStatus(client, streamName, StreamStatus.ACTIVE, timeout);
            if (initialStatus == null)
            {
                // probable timeout while waiting for stream to become active
                return null;
            }

            // there is a chance that status changes between the last describe and this one
            // but we'll simply let the update call fail rather than use an inner test

            DescribeStreamRequest describeRequest = new DescribeStreamRequest().withStreamName(streamName);
            StreamDescription description = describeStream(client, describeRequest, (timeoutAt - System.currentTimeMillis()));
            if (description == null)
            {
                // stream has magically disappeared between two calls or we've timed out
                return null;
            }
            if (getStatus(description) != StreamStatus.ACTIVE)
            {
                // race condition, someone else is changing stream so wait until they're done
                continue;
            }

            int currentRetentition = description.getRetentionPeriodHours().intValue();
            if (currentRetentition == retention)
            {
                // nothing to see here, move along
                return StreamStatus.ACTIVE;
            }

            try
            {
                if (currentRetentition > retention)
                {
                    DecreaseStreamRetentionPeriodRequest request = new DecreaseStreamRetentionPeriodRequest()
                                                                   .withStreamName(streamName)
                                                                   .withRetentionPeriodHours(retention);
                    client.decreaseStreamRetentionPeriod(request);
                    break;
                }
                else
                {
                    IncreaseStreamRetentionPeriodRequest request = new IncreaseStreamRetentionPeriodRequest()
                                                                   .withStreamName(streamName)
                                                                   .withRetentionPeriodHours(retention);
                    client.increaseStreamRetentionPeriod(request);
                    break;
                }
            }
            catch (LimitExceededException ex)
            {
                // fall out to the end of the loop
            }
            catch (ResourceInUseException ex)
            {
                // fall out to the end of the loop
            }
            catch (ResourceNotFoundException ex)
            {
                // stream doesn't exist so no point in trying to do anything more
                return null;
            }

            CommonUtils.sleepQuietly(currentSleep);
            currentSleep *= 2;
        }

        return waitForStatus(client, streamName, StreamStatus.ACTIVE, (timeoutAt - System.currentTimeMillis()));
    }


    /**
     *  Retrieves a single shard iterator, where the caller knows what type
     *  of iterator to retrieve.
     *
     *  @param  client          The AWS client used to make requests.
     *  @param  streamName      The name of the stream.
     *  @param  shardId         The shard identifier.
     *  @param  iteratorType    The type of iterator to retrieve.
     *  @param  sequenceNumber  The relevant sequence number, for iterator types
     *                          that need one. Ignored for iterator types that don't.
     *  @param  timestamp       A starting timestamp, for the AT_TIMESTAMP iterator
     *                          type only. Ignored for all other iterator types.
     *  @param  timeout         The number of milliseconds to attempt retries due
     *                          to throttling.
     *
     *  @return The shard iterator, null if unable to do so within timeout.
     *
     *  @throws All non-throttling exceptions from underlying call.
     *
     */
    public static String retrieveShardIterator(
        AmazonKinesis client, String streamName, String shardId,
        ShardIteratorType iteratorType, String sequenceNumber, Date timestamp,
        long timeout)
    {
        GetShardIteratorRequest request = new GetShardIteratorRequest()
                      .withStreamName(streamName)
                      .withShardId(shardId)
                      .withShardIteratorType(iteratorType);

        switch (iteratorType)
        {
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                request.setStartingSequenceNumber(sequenceNumber);
                break;
            case AT_TIMESTAMP:
                request.setTimestamp(timestamp);
                break;
            default:
                // nothing special
        }

        long timeoutAt = System.currentTimeMillis() + timeout;
        long currentSleepTime = 100;
        while (System.currentTimeMillis() < timeoutAt)
        {
            try
            {
                GetShardIteratorResult response = client.getShardIterator(request);
                return response.getShardIterator();
            }
            catch (ProvisionedThroughputExceededException ex)
            {
                CommonUtils.sleepQuietly(currentSleepTime);
                currentSleepTime *= 2;
            }
        }
        return null;
    }


    /**
     *  Retrieves shard iterators for the specified stream. The goal of this
     *  function is to allow uninterrupted reading of a stream based on saved
     *  sequence numbers. To do this it uses the following rules to determine
     *  which of a stream's shards to return, and what type of iterator to
     *  return for them:
     *
     *  <ul>
     *  <li> For shards that are in the sequence map:
     *       <ol>
     *       <li> If the shard is open (no ending sequence number), returns an
     *            <code>AFTER_SEQUENCE_NUMBER</code> iterator and ignores any
     *            parent shards in the stream.
     *       <li> If the shard is closed and the saved sequence number is less
     *            than the ending sequence number, returns an
     *            <code>AFTER_SEQUENCE_NUMBER</code> iterator and ignores any
     *            parent or child shards in the stream.
     *       <li> If the shard is closed, and the saved sequence number equals
     *            the ending sequence number of the shard, returns a
     *            <code>TRIM_HORIZON</code> iterator for any children that are
     *            not themselves in the map. This should be a rare occurrence.
     *       <li> If the shard is closed, the saved sequence number equals the
     *            ending sequence number of the shard, and there are no child
     *            shards in the stream, does not return an iterator. This will
     *            only happen if the map is well out-of-date and there have
     *            been multiple reshardings since the sequence numbers were saved.
     *       </ol>
     *  <li> When there are parent and child shards, neither of which are in the
     *       sequence map, one is chosen depending on default iterator type: the
     *       parent if <code>TRIM_HORIZON</code>, the child if <code>LATEST</code>
     *  <li> For shards that are not in the sequence map, and do not have parents
     *       that were handled by a previous rule, returns the default iterator.
     *  </ul>
     *
     *  To start reading a stream from its beginning, supply an empty map and
     *  a default iterator type of <code>TRIM_HORIZON</code>. To start reading
     *  a stream from its end, supply an empty map and a default iterator type
     *  <code>LATEST</code>.
     *
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  seqnums     A map associating shard ID with the sequence number
     *                      of the last record read from that shard. May be
     *                      null, in which case default iterators are retrieved
     *                      for all shards in the stream.
     *  @param  defaultType Specifies the default iterator type to use when the
     *                      shard is newly discovered.
     *  @param  timeout     Maximum number of milliseconds to spend trying to
     *                      retrieve iterators.
     *
     *  @return A map associating each shard ID with an iterator for that shard.
     *          Will be null if the stream does not exist or the timeout expired.
     */
    public static Map<String,String> retrieveShardIterators(
        AmazonKinesis client, String streamName,
        Map<String,String> seqnums, ShardIteratorType defaultType,
        long timeout)
    {
        long timeoutAt = System.currentTimeMillis() + timeout;

        // this handles the case where the user gives us a null map
        if (seqnums == null) seqnums = Collections.emptyMap();

        List<Shard> shards = describeShards(client, streamName, timeout);
        if (shards == null) return null;

        // this map lets us traverse parent-child relationships
        Map<String,Shard> shardsById = new HashMap<String,Shard>();
        for (Shard shard : shards)
        {
            shardsById.put(shard.getShardId(), shard);
        }

        // we work our way through parent-child relationships to find iterators
        Map<String,ShardIteratorType> retrieveTypes = new HashMap<String,ShardIteratorType>();
        for (Shard shard : shards)
        {
            if (! shardsById.containsKey(shard.getParentShardId()))
            {
                retrieveTypes.putAll(identifyIterators(shard, shardsById, seqnums, defaultType));
            }
        }

        Map<String,String> result = new HashMap<String,String>();
        for (Shard shard : shards)
        {
            String shardId = shard.getShardId();
            ShardIteratorType retrieveType = retrieveTypes.get(shard.getShardId());
            if (retrieveType == null)
                continue;

            long requestTimeout = timeoutAt - System.currentTimeMillis();
            if (requestTimeout < 0)
                return null;

            String shardIterator = retrieveShardIterator(client, streamName, shardId, retrieveType, seqnums.get(shardId), null, requestTimeout);
            if (shardIterator == null)
                return null;

            result.put(shardId, shardIterator);
        }

        return result;
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Finds the iterators that apply to a given shard and its descendents.
     *  This is called with an "ultimate parent" by retrieveShardIterators(),
     *  and recursively looks at that shard's descendents (if any).
     */
    private static Map<String,ShardIteratorType> identifyIterators(
        Shard shard, Map<String,Shard> shardsById, Map<String,String> seqnums, ShardIteratorType defaultType)
    {
        Map<String,ShardIteratorType> result = new HashMap<String,ShardIteratorType>();
        String shardId = shard.getShardId();

        if (seqnums.containsKey(shardId) && (! seqnums.get(shardId).equals(shard.getSequenceNumberRange().getEndingSequenceNumber())))
        {
            // we have a valid pointer into this shard so use it whether or not the shard has children
            result.put(shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            return result;
        }

        List<String> childIds = new ArrayList<String>();
        for (Shard ss : shardsById.values())
        {
            if (shardId.equals(ss.getParentShardId()))
                childIds.add(ss.getShardId());
        }

        if (childIds.isEmpty())
        {
            // no children, no saved sequence number, return default
            result.put(shardId, defaultType);
            return result;
        }

        for (String childId : childIds)
        {
            if (seqnums.containsKey(childId))
            {
                result.putAll(identifyIterators(shardsById.get(childId), shardsById, seqnums, defaultType));
            }
        }

        if (result.size() == childIds.size())
        {
            // we had stored sequence numbers for all children so we're done
            return result;
        }

        if (result.size() > 0)
        {
            // some children had stored offsets so we want to start reading at start of others
            for (String childId : childIds)
            {
                if (! result.containsKey(childId))
                {
                    result.put(childId, ShardIteratorType.TRIM_HORIZON);
                }
            }
            return result;
        }

        // no sequence number for this shard, no sequence numbers for any children, so either return oldest
        // possible iterator or latest possible iterator
        if (defaultType == ShardIteratorType.TRIM_HORIZON)
        {
            result.put(shardId, ShardIteratorType.TRIM_HORIZON);
        }
        else
        {
            for (String childId : childIds)
            {
                result.put(childId, ShardIteratorType.LATEST);
            }
        }

        return result;
    }

}
