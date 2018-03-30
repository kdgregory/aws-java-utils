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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  An instantiable class that represents a Kinesis stream as an iterable: each
 *  call to {@link #iterator} returns an iteration over unread messages in the
 *  stream. Its behavior departs from a "normal" <code>Iterable</code> in that
 *  each returned iterator only makes a single pass over the shards: to read
 *  the entire stream, you must make multiple calls to {@link #iterator}. This
 *  behavior provides the caller the opportunity to sleep (to avoid throttling)
 *  and also to save sequence numbers in order to continue reading later.
 *  <p>
 *  Example:
 *  <pre>
 *  </pre>
 *  <p>
 *  You construct the iterator by providing a map of sequence numbers by shard
 *  and a default iterator type. These are used to retrieve shard iterators
 *  according to the following rules:
 *  <ul>
 *  <li> If no offsets are provided, use the specified default iterator type
 *       for all "ultimate parent" shards (those that do not have parents in
 *       the list of shards). This is the normal case for starting to read a
 *       stream.
 *  <li> For each shard that has a sequence number in the map, create
 *       an <code>AFTER_SEQUENCE_NUMBER</code> iterator. This is the normal case
 *       for continuing to read the stream.
 *  <li> For the siblings of shards that have sequence numbers, but which do
 *       not themselves have sequence numbers, use the default iterator type.
 *       This covers the corner case where you start to read a stream but do
 *       not process all shards.
 *  <li> Ignore any shards that have children with saved sequence numbers. This
 *       handles the case where you do not purge old shards when saving offsets.
 *  </ul>
 *  <p>
 *  Within a shard, records are returned in order. However, shards are read in
 *  arbitrary order, so there is not guarantee of total record ordering (Kinesis
 *  cannot make that guarantee anyway, due to distribution and resend of incoming
 *  records).
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
    private long timeout;

    // these two maps drive our operation
    // while the javadoc says that shards are iterated in an arbitrary order, I
    // want a known order for testing; thus the internal maps are TreeMaps
    private Map<String,String> offsets = new TreeMap<String,String>();
    private Map<String,String> shardIterators;


//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  @param  client              Used to access Kinesis. Client configuration
     *                              determines the region containing the stream.
     *  @param  streamName          The stream to read.
     *  @param  offsets             A mapping of shard ID to the sequence number of the
     *                              most recent record read from that shard. The reader
     *                              will create an <code>AFTER_SEQUENCE_NUMBER</code>
     *                              iterator to read the shard.
     *  @param  defaultIteratorType The iterator type (<code>TRIM_HORIZON</code> or
     *                              <code>LATEST</code>) to use for shards that do not
     *                              have offsets in the map.
     *  @param  timeout             The number of milliseconds to attempt reads (including
     *                              shard iterators) before throwing.
     */
    public KinesisReader(
        AmazonKinesis client, String streamName, Map<String,String> offsets,
        ShardIteratorType defaultIteratorType, long timeout)
    {
        this.client = client;
        this.streamName = streamName;
        this.defaultIteratorType = defaultIteratorType;
        this.timeout = timeout;
        this.offsets.putAll(offsets);
    }


    /**
     *  Returns an iterator that reads all current shards in the stream. Each iterator
     *  makes a single pass through the shards, at which point the calling application
     *  should sleep to avoid throttling errors.
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
     *  Note: this map may contain offsets for both parent and child shards.
     */
    public Map<String,String> getOffsets()
    {
        return new TreeMap<String,String>(offsets);
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

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
         *  Returns the next record from the stream. Note that records read from
         *  different shards may not be read in the order they were written.
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

        //---------------------------------------------------------------------

        private void findNextShardWithRecords()
        {
            // this happens when the reader is first constructed
            if (shardIterators == null)
            {
                retrieveInitialIterators();
            }

            // this happens on the first call for each new iterator
            if (shardIds == null)
            {
                shardIds = new LinkedList<String>(shardIterators.keySet());
            }

            while (currentRecords.isEmpty() && (! shardIds.isEmpty()))
            {
                currentShardId = shardIds.removeFirst();
                readCurrentShard();
            }
        }


        /**
         *  Retrieves the initial shard iterators, using either the supplied offsets or the
         *  default iterator type. This will be called on the first iteration, as well as
         *  any time that we have expired iterators.
         */
        private void retrieveInitialIterators()
        {
            shardIterators = new ShardIteratorManager(client, streamName, offsets, defaultIteratorType, timeout)
                             .retrieveInitialIterators();
        }


        /**
         *  Called when we reach the end of a shard, to replace the parent shard iterator
         *  with TRIM_HORIZON iterators for each of its children.
         */
        private void retrieveChildIterators(String parentShardId)
        {
            shardIterators.remove(parentShardId);
            shardIterators.putAll(new ShardIteratorManager(client, streamName, offsets, defaultIteratorType, timeout)
                                  .retrieveChildIterators(parentShardId));
        }


        private void readCurrentShard()
        {
            String shardItx = shardIterators.get(currentShardId);

            // TODO - handle exceptions
            GetRecordsResult response = client.getRecords(new GetRecordsRequest().withShardIterator(shardItx));
            currentRecords.addAll(response.getRecords());
            // TODO - track millis behind latest

            String nextShardIterator = response.getNextShardIterator();
            shardIterators.put(currentShardId, nextShardIterator);
            if (nextShardIterator == null)
            {
                retrieveChildIterators(currentShardId);
            }
        }
    }


    /**
     *  This class encapsulates shard iterator retrieval, allowing for independent
     *  unit tests.
     */
    protected static class ShardIteratorManager
    {
        private AmazonKinesis client;
        private String streamName;
        private Map<String,String> initialOffsets;
        private ShardIteratorType defaultIteratorType;
        private long timeout;

        private List<Shard> shards;
        private Map<String,List<Shard>> shardsByParentId;

        public ShardIteratorManager(
            AmazonKinesis client, String streamName, Map<String,String> initialOffsets, ShardIteratorType defaultIteratorType, long timeout)
        {
            this.client = client;
            this.streamName = streamName;
            this.initialOffsets = initialOffsets;
            this.defaultIteratorType = defaultIteratorType;
            this.timeout = timeout;
        }

        public Map<String,String> retrieveInitialIterators()
        {
            long timeoutAt = System.currentTimeMillis() + timeout;
            describeShards();

            Map<String,ShardIteratorType> itxTypes = new HashMap<String,ShardIteratorType>();
            if (! constructMixedIteratorTree(shardsByParentId.get(null), itxTypes))
            {
                if (defaultIteratorType == ShardIteratorType.TRIM_HORIZON)
                {
                    for (Shard shard : shardsByParentId.get(null))
                    {
                        itxTypes.put(shard.getShardId(), defaultIteratorType);
                    }
                }
                else
                {
                    for (Shard shard : shards)
                    {
                        if (shardsByParentId.get(shard.getShardId()) == null)
                        {
                            itxTypes.put(shard.getShardId(), ShardIteratorType.LATEST);
                        }
                    }
                }
            }

            return retrieveIterators(itxTypes, timeoutAt);
        }

        public Map<String,String> retrieveChildIterators(String parentShardId)
        {
            long timeoutAt = System.currentTimeMillis() + timeout;
            describeShards();

            List<Shard> children = shardsByParentId.get(parentShardId);
            if (children == null)
            {
                return Collections.emptyMap();
            }

            Map<String,ShardIteratorType> itxTypes = new TreeMap<String,ShardIteratorType>();
            for (Shard shard : children)
            {
                itxTypes.put(shard.getShardId(), ShardIteratorType.TRIM_HORIZON);
            }

            return retrieveIterators(itxTypes, timeoutAt);
        }

        private void describeShards()
        {
            shards = KinesisUtils.describeShards(client, streamName, timeout);
            shardsByParentId = KinesisUtils.toMapByParentId(shards);
        }

        private Map<String,String> retrieveIterators(Map<String,ShardIteratorType> iteratorTypes, long timeoutAt)
        {
            Map<String,String> result = new TreeMap<String,String>();

            for (Map.Entry<String,ShardIteratorType> entry : iteratorTypes.entrySet())
            {
                String shardId = entry.getKey();
                ShardIteratorType iteratorType = entry.getValue();
                String offset = initialOffsets.get(shardId);
                long currentTimeout = timeoutAt - System.currentTimeMillis();
                // TODO - throw if < 0
                String shardIterator = KinesisUtils.retrieveShardIterator(client, streamName, shardId, iteratorType, offset, null, currentTimeout);
                // TODO - throw if null
                result.put(shardId, shardIterator);
            }
            return result;
        }

        private boolean constructMixedIteratorTree(List<Shard> children, Map<String,ShardIteratorType> itxTypes)
        {
            if (children == null)
                return false;

            Set<String> childrenWithOffsets = new HashSet<String>();
            for (Shard child : children)
            {
                String childShardId = child.getShardId();
                if (constructMixedIteratorTree(shardsByParentId.get(childShardId), itxTypes))
                {
                    childrenWithOffsets.add(childShardId);
                }
                else if (initialOffsets.containsKey(childShardId))
                {
                    itxTypes.put(childShardId, ShardIteratorType.AFTER_SEQUENCE_NUMBER);
                    childrenWithOffsets.add(childShardId);
                }
            }

            if (childrenWithOffsets.size() == 0)
                return false;

            if (childrenWithOffsets.size() == children.size())
                return true;

            for (Shard child : children)
            {
                String childShardId = child.getShardId();
                if (! childrenWithOffsets.contains(childShardId))
                {
                    itxTypes.put(childShardId, ShardIteratorType.TRIM_HORIZON);
                }
            }
            return true;
        }
    }
}
