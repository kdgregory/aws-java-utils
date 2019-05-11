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
import com.amazonaws.services.logs.model.DescribeLogGroupsRequest;
import com.amazonaws.services.logs.model.DescribeLogGroupsResult;
import com.amazonaws.services.logs.model.LogGroup;

/**
 *  Retrieves a listing of log groups, automatically handling pagination.
 */
public class LogGroupIterable
implements Iterable<LogGroup>
{
    private AWSLogs client;
    private String prefix;


    /**
     *  Iterates over log groups with a specified prefix.
     */
    public LogGroupIterable(AWSLogs client, String prefix)
    {
        this.client = client;
        this.prefix = prefix;
    }


    /**
     *  Iterates over all log groups.
     */
    public LogGroupIterable(AWSLogs client)
    {
        this(client, null);
    }


    @Override
    public Iterator<LogGroup> iterator()
    {
        return new LogGroupIterator();
    }


    private class LogGroupIterator
    implements Iterator<LogGroup>
    {
        private DescribeLogGroupsResult currentBatch;
        private Iterator<LogGroup> currentItx;

        @Override
        public boolean hasNext()
        {
            if ((currentItx != null) && currentItx.hasNext())
                return true;

            if ((currentBatch != null) && ((currentBatch.getNextToken() == null) || currentBatch.getNextToken().isEmpty()))
                return false;

            DescribeLogGroupsRequest request = new DescribeLogGroupsRequest();
            if (prefix != null)
                request.setLogGroupNamePrefix(prefix);

            if (currentBatch != null)
                request.setNextToken(currentBatch.getNextToken());

            currentBatch = client.describeLogGroups(request);
            currentItx = currentBatch.getLogGroups().iterator();

            return currentItx.hasNext();
        }

        @Override
        public LogGroup next()
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
