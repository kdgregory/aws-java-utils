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

package com.kdgregory.aws.utils.cloudwatch;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.model.*;

/**
 *  Retrieves a listing of log streams, automatically handling pagination.
 */
public class LogStreamIterable
implements Iterable<LogStream>
{
    private AWSLogs client;
    private String logGroupName;
    private String prefix;


    /**
     *  Iterates over log streams with a specified prefix.
     */
    public LogStreamIterable(AWSLogs client, String logGroupName, String prefix)
    {
        this.client = client;
        this.logGroupName = logGroupName;
        this.prefix = prefix;
    }


    /**
     *  Iterates over all log streams for a named log group.
     */
    public LogStreamIterable(AWSLogs client, String logGroupName)
    {
        this(client, logGroupName, null);
    }


    @Override
    public Iterator<LogStream> iterator()
    {
        return new LogStreamIterator();
    }


    private class LogStreamIterator
    implements Iterator<LogStream>
    {
        private DescribeLogStreamsResult currentBatch;
        private Iterator<LogStream> currentItx;

        @Override
        public boolean hasNext()
        {
            if ((currentItx != null) && currentItx.hasNext())
                return true;

            if ((currentBatch != null) && ((currentBatch.getNextToken() == null) || currentBatch.getNextToken().isEmpty()))
                return false;

            DescribeLogStreamsRequest request = new DescribeLogStreamsRequest(logGroupName);
            if ((prefix != null) && !prefix.isEmpty())
                request.setLogStreamNamePrefix(prefix);

            if (currentBatch != null)
                request.setNextToken(currentBatch.getNextToken());

            try
            {
                currentBatch = client.describeLogStreams(request);
                currentItx = currentBatch.getLogStreams().iterator();
                return currentItx.hasNext();
            }
            catch (ResourceNotFoundException ex)
            {
                return false;
            }
        }

        @Override
        public LogStream next()
        {
            if (hasNext())
                return currentItx.next();

            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException("can not delete log groups via iterator");
        }
    }
}
