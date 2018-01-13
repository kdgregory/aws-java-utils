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

import java.util.List;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  An instantiable class that accumulates batches of log messages and uploads
 *  them with <code>putRecords()</code>.
 */
public class KinesisWriter
{
    public KinesisWriter(AmazonKinesis client, String streamName)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }


    /**
     *  Checks the size of the passed message to verify that it can be sent (even if
     *  in a batch of 1).
     */
    public boolean checkMessageSize(String partitionKey, String data)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }


    /**
     *  Checks the size of the passed message to verify that it can be sent (even if
     *  in a batch of 1).
     */
    public boolean checkMessageSize(String partitionKey, byte[] data)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }


    /**
     *  Attempts to add a record to the current batch, encoding them using UTF-8.
     *  Returns true if able to do so, false if adding the record would exceed
     *  Amazon's batch size.
     */
    public boolean addRecord(String partitionKey, String data)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }


    /**
     *  Attempts to add a record to the current batch. Returns true if able to do so,
     *  false if adding the record would exceed Amazon's batch size.
     */
    public boolean addRecord(String partitionKey, byte[] data)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }


    /**
     *  Returns all records currently in the batch. These are the actual records to
     *  be sent, so any changes by the caller may cause errors in sending.
     */
    public List<Record> getUnsentRecords()
    {
        throw new UnsupportedOperationException("FIXME - implement");
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
        throw new UnsupportedOperationException("FIXME - implement");
    }


    /**
     *  Returns the raw results from sending the last batch of records. Each record in the
     *  result corresponds to a record in the pre-send list of records.
     */
    public List<PutRecordsResultEntry> getSendResults()
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
}
