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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;

import com.kdgregory.aws.utils.CommonUtils;


/**
 *  An instantiable class that accumulates batches of log messages and uploads
 *  them with <code>PutRecords</code>, retaining records that were throttled.
 *
 *  <h1> Partition Keys </h1>
 *
 *  Kinesis requires that each record be associated with a partition key, and
 *  uses that partition key to assign the record to a shard. For high-volume
 *  producers, a random partition key will maximize throughput (assuming that
 *  you have multiple shards). To support this, the {@link #addRecord} methods
 *  accept a null or empty partition key, and replace it with a random key
 *  (using the standard <code>Random</code> class -- there's no need for
 *  cryptographically-secure randomness for this application).
 *
 *  <h1> Error Handling and Send Failure </h1>
 *
 *  Kinesis can throttle <code>GetRecords</code> requests at both the request and
 *  individual record level; the first causes an exception while the second just
 *  sets an error code in the response for that record. In both cases the writer
 *  will retain the records for a future send attempt and write a WARN-level
 *  message to the log.
 *  <p>
 *  All other exceptions are propagated to the caller, with unsent messages retained
 *  for a future send attempt. This includes <code>KMSThrottlingException</code>: to
 *  treat it as "just throttling" would require adding a dependency that not all
 *  clients would need.
 *
 *  <h1> General Notes </h1>
 *
 *  Because Kinesis may fail individual records, which are then retained by the
 *  writer, there is no guarantee that the order of records in a shard will match
 *  the order that they were added to the writer.
 *  <p>
 *  Attempting to send without any records is assumed to be an appllication error,
 *  and results in a WARN-level log message. If you are doing this intentionally
 *  (for example, sending on an interval regardless of contents) you should call
 *  {@link #getUnsentRecords} and verify that it's non-empty before sending.
 *  <p>
 *  This class is <em>not</em> safe for concurrent use by multiple threads.
 *
 *  <h1> Example </h1>
 */
public class KinesisWriter
{
    private final static int    MAX_RECORD_SIZE         = 1024 * 1024;
    private final static int    MAX_RECORDS_PER_REQUEST = 500;
    private final static int    MAX_REQUEST_SIZE        = 5 * 1024 * 1024;

//----------------------------------------------------------------------------
//  Instance variables and constructor
//----------------------------------------------------------------------------

    private Log logger = LogFactory.getLog(getClass());

    private AmazonKinesis client;
    private String streamName;

    private Random partitionKeyRandomizer = new Random();

    private List<PutRecordsRequestEntry> unsentRecords = new ArrayList<PutRecordsRequestEntry>();
    private int unsentRecordsSize = 0;

    private List<PutRecordsResultEntry> lastSendResults = Collections.emptyList();


    public KinesisWriter(AmazonKinesis client, String streamName)
    {
        this.client = client;
        this.streamName = streamName;
    }

//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  Attempts to add a record to the current batch, encoding them using UTF-8.
     *  Returns true if able to do so, false if adding the record would exceed
     *  Amazon's batch size.
     *  <p>
     *  Throws a <code>RuntimeException</code> if unable to convert the string to
     *  UTF-8. This indicates a severe problem with the JVM, so there's no point
     *  in continuing.
     */
    public boolean addRecord(String partitionKey, String data)
    {
        return addRecord(partitionKey, CommonUtils.toUTF8(data));
    }


    /**
     *  Attempts to add a record to the current batch. Returns true if able to do so,
     *  false if adding the record would exceed Amazon's batch size.
     */
    public boolean addRecord(String partitionKey, byte[] data)
    {
        if (unsentRecords.size() >= MAX_RECORDS_PER_REQUEST)
            return false;

        if ((partitionKey == null) || partitionKey.isEmpty())
        {
            partitionKey = String.format("%08x", partitionKeyRandomizer.nextInt());
        }

        int partitionKeySize = CommonUtils.toUTF8(partitionKey).length;
        int dataSize = data.length;

        if (partitionKeySize + dataSize > MAX_RECORD_SIZE)
            return false;

        if (partitionKeySize + dataSize + unsentRecordsSize > MAX_REQUEST_SIZE)
            return false;

        unsentRecordsSize += partitionKeySize + dataSize;
        unsentRecords.add(new PutRecordsRequestEntry()
                           .withPartitionKey(partitionKey)
                           .withData(ByteBuffer.wrap(data)));
        return true;
    }


    /**
     *  Attempts to send the current batch, waiting for the response. Any batch-level
     *  exceptions will be propagated to the caller, and you can assume that none of
     *  the records were sent.
     *  <p>
     *  If the send is successful, individual records may have failed. These records are
     *  preserved for a subsequent send attempt; all successfully-sent records are removed
     *  from the batch.
     *  <p>
     *  After a send attempt you can call {@link #getSendResults} to return the status of
     *  each record in the batch.
     */
    public void send()
    {
        if (unsentRecords.isEmpty())
        {
            logger.warn("attempted empty send to " + streamName);
            return;
        }

        try
        {
            PutRecordsRequest request = new PutRecordsRequest()
                                        .withStreamName(streamName)
                                        .withRecords(unsentRecords);
            PutRecordsResult response = client.putRecords(request);
            lastSendResults = response.getRecords();
        }
        catch (ProvisionedThroughputExceededException ex)
        {
            logger.warn("provisioned throughput exceeded for stream " + streamName);
            return;
        }

        // we need to retain any individual records that failed; the easiest way to do
        // this is just reset the record cache ... but we need to create an iterator
        // on the old cache before doing that!

        Iterator<PutRecordsRequestEntry> requestEntryItx = unsentRecords.iterator();
        clear();

        int successCount = 0;
        int failureCount = 0;
        Map<String,Integer> failureCounts = new TreeMap<String,Integer>();
        Map<String,String> failureDetail = new TreeMap<String,String>();

        for (PutRecordsResultEntry resultEntry : lastSendResults)
        {
            PutRecordsRequestEntry requestEntry = requestEntryItx.next();
            if ((resultEntry.getErrorCode() != null) && (resultEntry.getErrorCode().length() > 0))
            {
                failureCount++;
                Integer count = failureCounts.get(resultEntry.getErrorCode());
                count = (count != null)
                      ? count + 1
                      : Integer.valueOf(1);
                failureCounts.put(resultEntry.getErrorCode(), count);
                failureDetail.put(resultEntry.getErrorCode(), resultEntry.getErrorMessage());

                addRecord(requestEntry.getPartitionKey(), requestEntry.getData().array());
            }
            else
            {
                successCount++;
            }
        }

        if (logger.isDebugEnabled())
        {
            logger.debug("sent " + successCount + " of " + (successCount + failureCount) + " records to " + streamName);
        }

        for (String errorType : failureCounts.keySet())
        {
            logger.warn("failed to send " + failureCounts.get(errorType) 
                        + " records to " + streamName + " due to " + errorType
                        + " (sample message: " + failureDetail.get(errorType) + ")");
        }
    }


    /**
     *  Deletes all unsent records.
     */
    public void clear()
    {
        // this is called as part of send() post-processing so we don't want to reuse
        // existing list
        unsentRecords = new ArrayList<PutRecordsRequestEntry>();
        unsentRecordsSize = 0;
    }


    /**
     *  Returns a list of all records currently in the batch. These are the actual records
     *  to be sent, so any changes by the caller may cause operational errors.
     */
    public List<PutRecordsRequestEntry> getUnsentRecords()
    {
        return unsentRecords;
    }


    /**
     *  Returns the calculated size of the current unsent records.
     */
    public int getUnsentRecordSize()
    {
        return unsentRecordsSize;
    }


    /**
     *  Returns a list containing the raw per-record results from the  last {@link #send}.
     *  This can be used to check detailed error codes or sequence numbers.
     */
    public List<PutRecordsResultEntry> getSendResults()
    {
        return lastSendResults;
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

}
