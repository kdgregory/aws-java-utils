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
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  An instantiable class that represents a Kinesis stream as an iterable: each
 *  call to {@link #iterator} returns an iteration over the unread messages in
 *  all active shards in the stream. Where this behavior departs from what you
 *  might expect is that you can iterate the stream again, which will pick up
 *  messages added since the last iteration.
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
 *  Within a shard, records will be returned in order. However, shards will be
 *  read in arbitrary order, so there is not guarantee of total record ordering
 *  (Kinesis cannot make that guarantee anyway, due to distribution and resend
 *  of incoming records).
 *  <p>
 *  Due to the way that Kinesis stores records, any particular iteration may
 *  not return anything. This is particularly noticeable when you're at the
 *  end of the stream: the <code>GetRecords</code> call will always return an
 *  empty list of records.
 *  <p>
 *  To avoid throttling, the caller should sleep for an appropriate interval
 *  between creating iterators (where "appropriate" depends on the number of
 *  clients that are reading the stream simultaneously). If you do not sleep,
 *  then the iteratormay throw <code>ProvisionedThroughputExceededException</code>.
 *  The caller is expected to catch this exception (although as a runtime exception
 *  there's no way to enforce this). You can continue to use the iterator if
 *  this exception is thrown: it will attempt to re-read the same shard. But
 *  if you don't sleep before reading you'll just get another exception.
 *  <p>
 *  Instances of this class and the iterators that they produce are <em>not</em>
 *  safe for concurrent use from multiple threads.
 */
public class KinesisReader
implements Iterable<Record>
{
    private AmazonKinesis client;
    private String streamName;
    private ShardIteratorType defaultIteratorType;

    // TODO - make this a configurable option
    private long timeout = 1000;

    // while the javadoc says that shards are iterated in an arbitrary order, I
    // want a known order for testing; thus the internal maps are TreeMaps
    private Map<String,String> offsets = new TreeMap<String,String>();
    private Map<String,String> shardIterators = new TreeMap<String,String>();


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
        this.client = client;
        this.streamName = streamName;
        this.defaultIteratorType = iteratorType;
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
        this.client = client;
        this.streamName = streamName;
        this.defaultIteratorType = ShardIteratorType.TRIM_HORIZON;
        this.offsets.putAll(offsets);
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
        return new RecordIterator();
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
        return new TreeMap<String,String>(offsets);
    }


    /**
     *  Each instance of this iterator will make a single pass through the active shards,
     *  making one <code>GetRecords</code> call for each.
     */
    private class RecordIterator implements Iterator<Record>
    {
        private LinkedList<String> shardIds;
        private String currentShardId;
        private LinkedList<Record> currentRecords = new LinkedList<Record>();


        /**
         *  Determines whether there are more records to be read from this iteration
         *  of the stream.
         */
        @Override
        public boolean hasNext()
        {
            if (currentRecords.isEmpty())
            {
                findNextShardWithRecords();
            }
            return ! currentRecords.isEmpty();
        }


        /**
         *  Returns the next record from the stream.
         */
        @Override
        public Record next()
        {
            if (! hasNext())
                throw new NoSuchElementException("no more records for this iteration of " + streamName);

            Record record = currentRecords.removeFirst();
            offsets.put(currentShardId, record.getSequenceNumber());
            return record;
        }


        /**
         *  Unsupported operation: cannot remove a record from a Kinesis stream.
         */
        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("Kinesis streams are immutable");
        }

        // internals

        private void findNextShardWithRecords()
        {
            if (shardIds == null)
            {
                shardIterators.putAll(KinesisUtils.retrieveShardIterators(client, streamName, offsets, defaultIteratorType, timeout));
                shardIds = new LinkedList<String>(shardIterators.keySet());
            }

            while (currentRecords.isEmpty() && (! shardIds.isEmpty()))
            {
                currentShardId = shardIds.removeFirst();
                readCurrentShard();
            }
        }


        private void readCurrentShard()
        {
            String shardItx = shardIterators.get(currentShardId);
            // TODO - return if iterator is null (happens at end of shard)
            // TODO - handle exceptions
            GetRecordsResult response = client.getRecords(new GetRecordsRequest().withShardIterator(shardItx));
            shardIterators.put(currentShardId, response.getNextShardIterator());
            // TODO - track millis behind latest
            currentRecords.addAll(response.getRecords());
        }
    }
}
