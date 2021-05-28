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

import java.util.Comparator;

import com.amazonaws.services.logs.model.OutputLogEvent;


/**
 *  A comparator for OutputLogEvents that sorts them by timestamp and treats null
 *  timestamps (which are allowed, per <code>GetLogEvents</code> doc) as zero.
 */
public class OutputLogEventComparator
implements Comparator<OutputLogEvent>
{
    private boolean isIncreasing;

    /**
     *  Default constructor; sorts by increasing timestamp.
     */
    public OutputLogEventComparator()
    {
        this(true);
    }

    /**
     *  Base constructor: allows sorting by increasing (true) or decreasing
     *  (false) timestamp.
     */
    public OutputLogEventComparator(boolean isIncreasing)
    {
        this.isIncreasing = isIncreasing;
    }

    @Override
    public int compare(OutputLogEvent e1, OutputLogEvent e2)
    {
        // per docs, events do not need to have a timestamp
        Long ts1 = (e1.getTimestamp() != null) ? e1.getTimestamp() : Long.valueOf(0);
        Long ts2 = (e2.getTimestamp() != null) ? e2.getTimestamp() : Long.valueOf(0);
        int cmp = ts1.compareTo(ts2);
        return isIncreasing ? cmp : -cmp;
    }
}