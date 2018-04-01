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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  An instantiable class that represents a Kinesis stream as an iterable: each
 *  call to {@link #iterator} returns an iteration over a set of unread messages
 *  from the stream. It is intended for the use case where each consumer processes
 *  all messages in the stream, <em>not</em> for the case where there are multiple
 *  coordinated consumers reading the same stream.
 *  <p>
 *  Unlike a "normal" <code>Iterable</code>, the returned iterator does not read
 *  the entire stream. Instead, it calls <code>GetRecords</code> once for each
 *  shard in the stream, and then reports no more elements. This allows the caller
 *  to sleep between calls (to avoid throttling), as well as providing a good spot
 *  to retrieve and save sequence numbers.
 *
 *  <h1> Construction and Configuration </h1>
 *
 *  The sole constructor takes just two arguments: a Kinesis client object and the
 *  name of the stream to be read. This produces a reader that starts reading at
 *  the current end of stream. Specifically, it identifies all open shards in the
 *  stream creates a <code>LATEST</code> shard iterator for them.
 *  <p>
 *  This behavior can be altered via several configuration methods, each of which
 *  can be chained.
 *
 *  <h1> Sequence Numbers </h1>
 *
 *  To support applications that must start and stop, the reader retains the last
 *  sequence number read from each shard. You can retrieve the current set of
 *  sequence numbers with {@link #getCurrentSequenceNumbers()}, and use them to
 *  configure a new reader via {@link #withInitialSequenceNumbers(Map)}.
 *  <p>
 *  When you configure a reader with initial sequence numbers, that changes the
 *  behavior of the reader:
 *  <ul>
 *  <li> If none of the <em>current</em> shards in the stream correspond to the
 *       saved sequence numbers, then the reader uses the default shard iterator
 *       type. This covers the case where saved sequence numbers are long out of
 *       date.
 *  <li> For any shard that has a saved sequence number, <em>provided that there
 *       are no descendent shards with saved sequence numbers,</em> the reader
 *       creates an <code>AFTER_SEQUENCE_NUMBER</code> iterator based on the
 *       saved sequence number.
 *  <li> If there are saved sequence numbers for a shard and its descendents, only
 *       the descendent sequence numbers are retained. This prevents re-reading a
 *       shard once we've started reading its children.
 *  <li> If there is a saved sequence number for a shard but not its sibling, then
 *       the sibling is given a <code>TRIM_HORIZON</code> iterator. This covers
 *       the case where a reader was interrupted before it could make a pass of all
 *       shards.
 *
 *  <h1> Error Handling </h1>
 *
 *  <h1> General Notes </h1>
 *
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
 *  clients that are reading the stream simultaneously). As described above,
 *  throttling exceptions may be transparently handled via retries, but the
 *  application should not rely on this behavior.
 *  <p>
 *  Instances of this class and the iterators that they produce are <em>not</em>
 *  safe for concurrent use from multiple threads.
 *
 *  <h1> Example </h1>
 */
public class KinesisReader
implements Iterable<Record>
{
    private Log logger = LogFactory.getLog(getClass());

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
//  Constructor and Configuration Methods
//----------------------------------------------------------------------------

    /**
     *  Constructs an instance that starts reading the stream at its current end.
     *  Call one or more of the configuration methods to change this behavior.
     *
     *  @param  client              Used to access Kinesis. Client configuration
     *                              determines the region containing the stream.
     *  @param  streamName          The stream to read.
     */
    public KinesisReader(AmazonKinesis client, String streamName)
    {
        this.client = client;
        this.streamName = streamName;
        this.timeout = 2500;
        this.defaultIteratorType = ShardIteratorType.LATEST;
        this.offsets.putAll(offsets);
    }


    /**
     *  Configures the stream to start reading at the first record. Specifically,
     *  identifies all shards that do not have parents, and creates a
     *  <code>TRIM_HORIZON</code> iterator for them.
     *  <p>
     *  Note that this method has no effect if you call {@link #withInitialSequenceNumbers}
     *  with a non-empty map.
     */
    public KinesisReader readFromTrimHorizon()
    {
        this.defaultIteratorType = ShardIteratorType.TRIM_HORIZON;
        return this;
    }


    /**
     *  Sets the initial sequence numbers for the stream's shards from the passed
     *  map: keys are shard IDs, values are sequence numbers. If the map is empty
     *  then this method acts as a no-op and reader behavior depends on default
     *  iterator type.
     */
    public KinesisReader withInitialSequenceNumbers(Map<String,String> value)
    {
        this.offsets = value;
        return this;
    }


    /**
     *  Sets the timeout, in milliseconds, for Kinesis interaction. The default
     *  value of 2500 should be sufficient for most usages, but a large number
     *  of shards may require a higher value to avoid timing out while loading
     *  shard iterators.
     */
    public KinesisReader withTimeout(long millis)
    {
        this.timeout = millis;
        return this;
    }


//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

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
     */
    public Map<String,String> getCurrentSequenceNumbers()
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
                // TODO - purge offsets for any shards that aren't in this list
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
            if (logger.isDebugEnabled())
            {
                logger.debug("retrieveInitialIterators: stream = " + streamName + ", offsets = " + offsets);
            }
            shardIterators = new ShardIteratorManager(client, streamName, offsets, defaultIteratorType, timeout)
                             .retrieveInitialIterators();
        }


        /**
         *  Called when we reach the end of a shard, to replace the parent shard iterator
         *  with TRIM_HORIZON iterators for each of its children.
         */
        private void retrieveChildIterators(String parentShardId)
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("retrieveChildIterators: stream = " + streamName + ", parent = " + parentShardId);
            }
            shardIterators.remove(parentShardId);
            shardIterators.putAll(new ShardIteratorManager(client, streamName, offsets, defaultIteratorType, timeout)
                                  .retrieveChildIterators(parentShardId));
        }


        private void readCurrentShard()
        {
            if (logger.isDebugEnabled())
            {
                logger.debug("readCurrentShard: stream = " + streamName + ", shard = " + currentShardId);
            }

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
        private Log logger = LogFactory.getLog(getClass());

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

            if (logger.isDebugEnabled())
            {
                logger.debug("retrieveInitialIterators: stream = " + streamName + ", iterator types = " + itxTypes);
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
