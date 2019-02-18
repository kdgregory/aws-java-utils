// Copyright (c) Keith D Gregory
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.kdgregory.aws.utils.logs;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

/**
 *  Writes messages to CloudWatch Logs in batches, either under application control
 *  or using a background thread. Messages may be added concurrently by any thread.
 *  Any thread may also invoke {@link #flush}; invocations are synchronized.
 *  <p>
 *
 */
public class CloudWatchWriter
{
    private Log logger = LogFactory.getLog(getClass());

    private AWSLogs client;
    private String logGroupName;
    private String logStreamName;

    private LinkedBlockingDeque<QueuedMessage> unsentMessages = new LinkedBlockingDeque<QueuedMessage>();


    /**
     *  Creates an instance that is called entirely under program control.
     *
     *  @param  client          The service client. AWS best practice is to share a single
     *                          client instance between all consumers.
     *  @param  logGroupName    The destination log group. This group will be created by
     *                          {@link #flush} if it does not exist (or has been deleted
     *                          since the last call).
     *  @param  logStreamName   The destination log group. This group will be created by
     *                          {@link #flush} if it does not exist (or has been deleted
     *                          since the last call).
     */
    public CloudWatchWriter(AWSLogs client, String logGroupName, String logStreamName)
    {
        this.client = client;
        this.logGroupName = logGroupName;
        this.logStreamName = logStreamName;
    }

//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  Adds a new message to the queue, with current timestamp.
     *
     *  @throws IllegalArgumentException if the message is too large to fit in
     *          a batch (even by itself).
     */
    public void add(String message)
    {
        add(System.currentTimeMillis(), message);
    }


    /**
     *  Adds a message to the queue, with specified timestamp.
     *
     *  @throws IllegalArgumentException if the message it too large to fit in
     *          a batch (even by itself) or the timestamp does not within the
     *          allowable range. Note that a message may still be rejected if
     *          the timestamp is older than the queue's retention period (this
     *          is checked by {@link #flush}).
     */
    public void add(long timestamp, String message)
    {
        long earliestAllowedTimestamp = System.currentTimeMillis() - 14 * 86400000;
        if (timestamp < earliestAllowedTimestamp)
            throw new IllegalArgumentException(
                        "message timestamp too far in past: " + timestamp
                        + " (limit: " + earliestAllowedTimestamp + ")");

        long latestAllowedTimestamp = System.currentTimeMillis() + 2 * 3600000;
        if (timestamp > latestAllowedTimestamp)
            throw new IllegalArgumentException(
                        "message timestamp too far in future: " + timestamp
                        + " (limit: " + latestAllowedTimestamp + ")");

        QueuedMessage qmessage = new QueuedMessage(timestamp, message);

        long sizeLimit = 1024 * 1024 - 26;
        if (qmessage.size > sizeLimit)
            throw new IllegalArgumentException(
                        "message is too large: " + qmessage.size + " bytes"
                        + " (limit: " + sizeLimit + ")");

        unsentMessages.add(qmessage);
    }


    /**
     *  Flush outstanding messages. Will build and send batches until the queue
     *  is empty or an uncrecoverable error occurs.
     */
    public synchronized void flush()
    {
        LinkedList<QueuedMessage> messages = extractAndSortMessages();
        if (messages.isEmpty())
            return;

        try
        {
            String uploadSequenceToken = retrieveUploadSequenceToken();
            while ((messages.size() > 0) && (uploadSequenceToken != null))
            {
                List<QueuedMessage> batch = extractBatch(messages);
                try
                {
                    uploadSequenceToken = attemptToSend(batch, uploadSequenceToken);
                    batch.clear();
                }
                finally
                {
                    // there will only be messages remaining if there was an exception
                    unsentMessages.addAll(messages);
                }
            }
        }
        finally
        {
            unsentMessages.addAll(messages);
        }
    }


    /**
     *  Shuts down the writer: no more messages will be accepted, any current
     *  messages are flushed immediately, and not further tasks will be
     *  submitted to the executor (if any).
     */
    public void shutdown()
    {
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Retrieves the current upload sequence token for the stream. If the
     *  stream does not exist, will attempt to re-create it.
     */
    private String retrieveUploadSequenceToken()
    {
        LogStream stream = CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName);
        if (stream == null)
        {
            logger.debug("stream " + logGroupName + "/" + logStreamName + " does not exist, creating");
            stream = CloudWatchLogsUtil.createLogStream(client, logGroupName, logStreamName, 30000);
            if (stream == null)
            {
                logger.warn("stream " + logGroupName + "/" + logStreamName + " not ready after creation");
                return null;
            }
        }

        return stream.getUploadSequenceToken();
    }


    /**
     *  Pulls all messages out of the queue, sorts them, and converts them
     *  to <code>InputLogEvent</code>s.
     */
    private LinkedList<QueuedMessage> extractAndSortMessages()
    {
        // TODO - check timestamps against group's discard policy
        LinkedList<QueuedMessage> messages = new LinkedList<QueuedMessage>(unsentMessages);
        Collections.sort(messages, new MessageComparator());
        return messages;
    }


    /**
     *  Given a list of sorted messages, builds a batch from the oldest messages
     *  (removing them from the list).
     */
    private List<QueuedMessage> extractBatch(LinkedList<QueuedMessage> sortedMessages)
    {
        LinkedList<QueuedMessage> batch = new LinkedList<QueuedMessage>();
        if (sortedMessages.isEmpty())
            return batch;

        long lastAllowedTimestamp = sortedMessages.getFirst().timestamp + 86399999;
        int batchMessages = 0;
        int batchBytes = 0;

        for (Iterator<QueuedMessage> srcItx = sortedMessages.iterator() ; srcItx.hasNext() ; )
        {
            QueuedMessage message = srcItx.next();
            batchMessages++;
            batchBytes += message.size + 26;

            if ((batchMessages > 10000) || (batchBytes > 1048576) || (message.timestamp > lastAllowedTimestamp))
                return batch;

            batch.add(message);
            srcItx.remove();
        }

        return batch;
    }


    /**
     *  Attempts to send a batch of messages to the service. If successful, returns
     *  the sequence token for the next batch. Allows any exceptions to propagate.
     */
    private String attemptToSend(List<QueuedMessage> batch, String uploadSequenceToken)
    {
        List<InputLogEvent> events = new ArrayList<InputLogEvent>(batch.size());
        for (QueuedMessage message : batch)
        {
            events.add(new InputLogEvent()
                      .withTimestamp(message.timestamp)
                      .withMessage(message.message));
        }


        PutLogEventsRequest request = new PutLogEventsRequest(logGroupName, logStreamName, events)
                                      .withSequenceToken(uploadSequenceToken);
        PutLogEventsResult response = client.putLogEvents(request);
        return response.getNextSequenceToken();
    }


    /**
     *  Holder for a message. We retain both the source string and the UTF-8
     *  representaton of that string for debugging. All fields are public
     *  because we only use it internally (if you want to create a subclass
     *  and muck with it, feel free).
     */
    protected static class QueuedMessage
    {
        public long timestamp;
        public String message;
        public int size;

        public QueuedMessage(long timestamp, String message)
        {
            this.timestamp = timestamp;
            this.message = message;
            try
            {
                this.size = message.getBytes("UTF-8").length;
            }
            catch (UnsupportedEncodingException e)
            {
                throw new RuntimeException("this JVM does not support UTF-8 (should never happen)");
            }
        }
    }


    /**
     *  Used to sort messages: CloudWatch Logs will only accept messages that
     *  are ordered by timestamp.
     */
    protected static class MessageComparator
    implements Comparator<QueuedMessage>
    {
        @Override
        public int compare(QueuedMessage m1, QueuedMessage m2)
        {
            return (m1.timestamp < m2.timestamp) ? -1
                 : (m1.timestamp > m2.timestamp) ? 1
                 : 0;
        }
    }

}
