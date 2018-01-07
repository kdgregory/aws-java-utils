// Copyright (c) Keith D Gregory, all rights reserved
package com.kdgregory.aws.utils.kinesis;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;

/**
 *  General utility methods for Kinesis.
 */
public class KinesisUtils
{
    /**
     *  A wrapper around <code>DescribeStream</code> that retrieves all shard
     *  descriptions and retries if throttled. Returns null if the stream does
     *  not exist.
     *  
     *  @param  client      The AWS client used to make requests.
     *  @param  timeout     The total number of milliseconds to attempt retries. This
     *                      method will retry the operation every 100 milliseconds
     *                      until this timeout expires.
     */
    public static List<Shard> describeShards(AmazonKinesis client, long timeout)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
    
    
    /**
     *  Retrieves the named stream's status, retrying if rate-limited. Returns the
     *  status, null if the stream does not exist.
     */
    public static String retrieveStreamStatus(AmazonKinesis client, String streamName)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
    
    
    /**
     *  Creates a stream and waits for it to become available.
     *  
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  numShards   The number of shards to create.
     */
    public static void createStream(AmazonKinesis client, String streamName, int numShards)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
    
    
    /**
     *  Creates a stream with non-standard retention period and waits for 
     *  it to become available.
     *  
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  numShards   The number of shards to create.
     *  @param  retention   The retention period (hours)
     */
    public static void createStream(AmazonKinesis client, String streamName, int numShards, int retention)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }


    /**
     *  Deletes a stream and waits for it to no longer be available.
     *  
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     */
    public static void deleteStream(AmazonKinesis client, String streamName)
    {
        throw new UnsupportedOperationException("FIXME - implement");
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
     *       will return a <code>TRIM_HORIZON</code> iterator (so reading will
     *       start with the first untrimmed record in the stream).
     *  <li> For any shard in the stream that is not represented in the map, this
     *       function will return a <code>TRIM_HORIZON</code> iterator (so will
     *       identify new shards).
     *  </ul>
     *  To start reading a stream from its beginning, supply an empty map. 
     *  
     *  @param  client      The AWS client used to make requests.
     *  @param  streamName  The name of the stream.
     *  @param  seqnums     A map associating shard ID with the sequence number
     *                      of the last record read from that shard.
     *                      
     *  @return A map associating shard ID with an iterator for that shard.                    
     */
    public static Map<String,String> retrieveShardIterators(
        AmazonKinesis client, String streamName, Map<String,String> seqnums)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
}
