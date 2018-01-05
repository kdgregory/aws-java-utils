// Copyright (c) Keith D Gregory, all rights reserved
package com.kdgregory.aws.utils.kinesis;

import java.util.List;

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
    public static List<Shard> describeStream(AmazonKinesis client, long timeout)
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
}
