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

import com.amazonaws.services.logs.model.InputLogEvent;


/**
 *  A comparator for <code>InputLogEvents</code>, as preparation for calling
 *  <code>PutLogEvents</code>. Events are ordered by timestamp, increasing.
 *  Events with a null timestamp will cause <code>NullPointerException</code>.
 */
public class InputLogEventComparator
implements Comparator<InputLogEvent>
{
    @Override
    public int compare(InputLogEvent e1, InputLogEvent e2)
    {
        return e1.getTimestamp().compareTo(e2.getTimestamp());
    }
}