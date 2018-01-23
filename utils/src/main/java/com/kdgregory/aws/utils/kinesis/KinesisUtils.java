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
     *  Retrieves shard iterators for the specified stream. The goal of this
     *  function is to allow uninterrupted reading of a stream based on saved
     *  offsets, so the following rules are applied to the results:
     *  <ul>
     *  <li> For any currently-open shard that is in the map, this function
     *       will return an <code>AFTER_SEQUENCE_NUMBER</code> iterator (this
     *       is the normal case).
     *  <li> For any closed shard that is in the map, this function will return
     *       an <code>AFTER_SEQUENCE_NUMBER</code> iterator if-and-only-if the
     *       last sequence number in the shard's range is greater than that in
     *       the supplied map.
     *  <li> For any closed shard that is in the map, where the last sequence
     *       number in the shard is equal to the number in the map, this function
     *       will return a <code>TRIM_HORIZON</code> iterator for each of the
     *       shard's children.
     *  <li> For any open shard in the stream that is not in the map and does not
     *       have a parent in the map, returns an iterator using the provided
     *       default iterator type.
     *  </ul>
     *  To start reading a stream from its beginning, supply an empty map.
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

        // the user might not have supplied offsets but we still need to query the map
        if (seqnums == null) seqnums = Collections.emptyMap();

        List<Shard> shards = describeShards(client, streamName, timeout);
        if (shards == null) return null;

        Map<String,ShardIteratorType> retrieveTypes = identifyShardsToRetrieve(shards, seqnums, defaultType);

        Map<String,String> result = new HashMap<String,String>();
        int shardsProcessed = 0;
        for (Shard shard : shards)
        {
            String shardId = shard.getShardId();
            ShardIteratorType retrieveType = retrieveTypes.get(shard.getShardId());
            if (retrieveType == null)
                continue;

            long currentSleepTime = 100;
            while (System.currentTimeMillis() < timeoutAt)
            {
                try
                {
                    GetShardIteratorRequest request = new GetShardIteratorRequest()
                                  .withStreamName(streamName)
                                  .withShardId(shardId)
                                  .withShardIteratorType(retrieveType);
                    if (retrieveType == ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                    {
                        request.setStartingSequenceNumber(seqnums.get(shardId));
                    }

                    GetShardIteratorResult response = client.getShardIterator(request);
                    result.put(shardId, response.getShardIterator());
                    shardsProcessed++;
                    break;
                }
                catch (ProvisionedThroughputExceededException ex)
                {
                    CommonUtils.sleepQuietly(currentSleepTime);
                    currentSleepTime *= 2;
                }
            }
        }

        return shardsProcessed == retrieveTypes.size()
             ? result
             : null;
    }


//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  This function is called by retrieveShardIterators() to figure out which
     *  shards to retrieve and how to retrieve them. The returned map only holds
     *  shards that we should retrieve, so that it can be used to verify that we
     *  retrieved all shards that we should.
     */
    private static Map<String,ShardIteratorType> identifyShardsToRetrieve(
        List<Shard> shards, Map<String,String> seqnums, ShardIteratorType defaultType)
    {
        // this map lets us follow parent-child relationships
        Map<String,Shard> shardsById = new HashMap<String,Shard>();
        for (Shard shard : shards)
        {
            shardsById.put(shard.getShardId(), shard);
        }

        Map<String,ShardIteratorType> result = new HashMap<String,ShardIteratorType>();
        for (Shard shard : shards)
        {
            String shardId = shard.getShardId();
            String savedOffset = seqnums.get(shardId);

            if (savedOffset == null)
            {
                result.put(shardId, defaultType);
            }
            else if (! savedOffset.equals(shard.getSequenceNumberRange().getEndingSequenceNumber()))
            {
                result.put(shardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER);
            }
        }
        return result;
    }
}
