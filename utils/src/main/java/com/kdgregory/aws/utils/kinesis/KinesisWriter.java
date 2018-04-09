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
 *  <h1> Example </h1> *  <h1> Example </h1>
 *
 *  This example reads from a <code>BufferedReader</code> and sends each line as
 *  a separate record using a random partition key. Note that <code>send()</code>
 *  is triggered when we can't write the record. Note also the sleep after each
 *  send, the second attempt to write the record (with possible error), and the
 *  loop at the end to ensure that all records are sent.
 *
 *  <pre>
 *  AmazonKinesis client = AmazonKinesisClientBuilder.defaultClient();
 *  KinesisWriter writer = new KinesisWriter(client, "example");
 *
 *  BufferedReader in = // get it from somewhere, remember to close it
 *  String inputLine = "";
 *  while ((inputLine = in.readLine()) != null) {
 *      if (! writer.addRecord(null, inputLine)) {
 *          writer.send();
 *          Thread.sleep(1000);
 *      }
 *      if (! writer.addRecord(null, inputLine)) {
 *          System.err.println("sending did not make room for new record");
 *          // we'll drop this record
 *      }
 *  }
 *
 *  while (writer.getUnsentRecords().size() > 0) {
 *      writer.send();
 *      Thread.sleep(1000);
 *  }
 *  </pre>
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
     *  Attempts to send the current batch. This may or may not be successful: individual
     *  records may be throttled, the entire batch may be throttled, or other exceptions
     *  may occur. In the case of throttling or individual record errors, the call apears
     *  to be successful; other exceptions are propagated to the caller. In either case,
     *  unsent records are retained by the writer and may be resent.
     *  <p>
     *  After sending, you may call {@link #getSendResults} to see the raw results from
     *  Kinesis (this is useful if you need sequence numbers or want to examine error
     *  messages). Note, however, that these responses correspond to the <em>original</em>
     *  records sent; you must call {@link #getUnsentRecords} <em>before</em> sending
     *  to correlate response with request.
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
     *  Attempts to send the current batch of records, calling {@link #send} with a delay
     *  until they're all sent or the passed timeout expires. Call {@link #getUnsentRecords}
     *  to determine whether all records were successfully sent.
     */
    public void sendAll(long timeoutMillis)
    {
        long timeoutAt = System.currentTimeMillis() + timeoutMillis;
        while ((unsentRecords.size() > 0) && (System.currentTimeMillis() < timeoutAt))
        {
            send();
            if (unsentRecords.size() > 0)
            {
                CommonUtils.sleepQuietly(250);
            }
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
