// Copyright (c) Keith D Gregory, all rights reserved
package com.kdgregory.aws.utils.kinesis;

import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;


/**
 *  An instantiable class that represents a Kinesis stream as an iterable,
 *  and provides support for saving the current shard offsets.
 */
public class KinesisReader
implements Iterable<Record>
{
    public KinesisReader(AmazonKinesis client, String streamName, Map<String,String> offsets, ShardIteratorType defaultIteratorType)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }

    
    @Override
    public Iterator<Record> iterator()
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
}
