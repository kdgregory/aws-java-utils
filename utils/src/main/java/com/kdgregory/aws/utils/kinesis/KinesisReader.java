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

import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;


/**
 *  An instantiable class that represents a Kinesis stream as an iterable: each
 *  call to {@link #iterator} returns an iteration over the unread messages in
 *  all active shards in the stream. Where this behavior departs from what you
 *  might expect is that you can iterate the stream again, which will pick up
 *  messages added since the last iteration. To avoid throttling errors, it is
 *  the responsibility of the caller to sleep between iterations.
 *  <p>
 *  You construct the reader in one of two ways: either with a default iterator
 *  type or a map of offsets by shard. The first is intended for when you first
 *  start reading the stream: specify <code>TRIM_HORIZON</code> to start with
 *  the oldest records in the stream, <code>LATEST</code> to start with the newest.
 *  The latter supports continued reading from the stream, after calling {@link
 *  #getOffsets}.
 *  <p>
 *  Example:
 *  <pre>
 *  </pre>
 *  <p>
 *  Instances of this class and the iterators that they produce are not thread-safe.
 */
public class KinesisReader
implements Iterable<Record>
{
    /**
     *  Creates an instance that starts reading from either the oldest or newest
     *  records in the stream, depending on <code>iteratorType</code>.
     *  
     *  @param client               Used to access Kinesis. Client configuration
     *                              determines the region containing the stream.
     *  @param streamName           The stream to read.
     *  @param defaultIteratorType  Pass either <code>TRIM_HORIZON</code>, to start reading
     *                              the oldest records in the stream, or <code>LATEST</code>,
     *                              to start reading at the newest.
     */
    public KinesisReader(AmazonKinesis client, String streamName, ShardIteratorType iteratorType)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
    
    
    /**
     *  Creates an instance that starts reading from specific positions provided in
     *  the offsets map. If a shard does not have offsets in the map, we start with
     *  the oldest records in the shard (iterator type <code>TRIM_HORIZON</code>).
     *  
     *  @param client               Used to access Kinesis. Client configuration
     *                              determines the region containing the stream.
     *  @param streamName           The stream to read.
     *  @param offsets              A mapping of shard ID to the sequence number of the
     *                              most recent record read from that shard. The reader 
     *                              will create an <code>AFTER_SEQUENCE_NUMBER</code>
     *                              iterator to read the shard.
     */
    public KinesisReader(AmazonKinesis client, String streamName, Map<String,String> offsets)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }

    
    /**
     *  Returns an iterator that reads all current shards in the stream. Depending on the
     *  iterator type, these shards may be open or closed. Shards are read in sequence by
     *  shard ID. Each iterator makes a single pass through the shards, at which point the
     *  calling application should sleep to avoid throttling errors.
     */
    @Override
    public Iterator<Record> iterator()
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
    
    
    /**
     *  Returns a map of the most recent sequence numbers read for each shard. This can
     *  be saved and used to initialize a new reader. The returned map is a copy of the
     *  current reader offsets; the caller may do with it whatever they want.
     *  <p>
     *  Note: this map does not purge closed shards; if you frequently re-shard your stream
     *  it can grow quite large.
     */
    public Map<String,String> getOffsets()
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
}
