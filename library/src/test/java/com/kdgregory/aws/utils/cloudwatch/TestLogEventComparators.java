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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.OutputLogEvent;


public class TestLogEventComparators
{

    @Test
    public void testInputLogEventComparator() throws Exception
    {
        InputLogEvent e1 = new InputLogEvent().withTimestamp(10L).withMessage("foo");
        InputLogEvent e2 = new InputLogEvent().withTimestamp(20L).withMessage("bar");
        InputLogEvent e3 = new InputLogEvent().withTimestamp(30L).withMessage("baz");
        InputLogEvent e4 = new InputLogEvent().withTimestamp(40L).withMessage("biff");
        InputLogEvent e5 = new InputLogEvent().withMessage("nothing");

        List<InputLogEvent> a1 = new ArrayList<>(Arrays.asList(e3, e2, e4, e1));

        Collections.sort(a1, new InputLogEventComparator());
        assertEquals("default constructor, no null timestamps",
                     Arrays.asList(e1, e2, e3, e4),
                     a1);

        try
        {
            List<InputLogEvent> a2 = new ArrayList<>(Arrays.asList(e3, e2, e5, e4, e1));
            Collections.sort(a2, new InputLogEventComparator());
        }
        catch (NullPointerException ex)
        {
            // success
        }
    }


    @Test
    public void testOutputLogEventComparator() throws Exception
    {
        OutputLogEvent e1 = new OutputLogEvent().withTimestamp(10L).withMessage("foo");
        OutputLogEvent e2 = new OutputLogEvent().withTimestamp(20L).withMessage("bar");
        OutputLogEvent e3 = new OutputLogEvent().withTimestamp(30L).withMessage("baz");
        OutputLogEvent e4 = new OutputLogEvent().withTimestamp(40L).withMessage("biff");
        OutputLogEvent e5 = new OutputLogEvent().withMessage("nothing");

        List<OutputLogEvent> a1 = new ArrayList<>(Arrays.asList(e3, e2, e4, e1));

        Collections.sort(a1, new OutputLogEventComparator());
        assertEquals("default constructor, no null timestamps",
                     Arrays.asList(e1, e2, e3, e4),
                     a1);

        Collections.sort(a1, new OutputLogEventComparator(false));
        assertEquals("reverse comparator, no null timestamps",
                     Arrays.asList(e4, e3, e2, e1),
                     a1);

        List<OutputLogEvent> a2 = new ArrayList<>(Arrays.asList(e3, e2, e5, e4, e1));

        Collections.sort(a2, new OutputLogEventComparator());
        assertEquals("default constructor, with null timestamp",
                     Arrays.asList(e5, e1, e2, e3, e4),
                     a2);

        Collections.sort(a2, new OutputLogEventComparator(false));
        assertEquals("reverse comparator, with null timestamp",
                     Arrays.asList(e4, e3, e2, e1, e5),
                     a2);
    }
}
