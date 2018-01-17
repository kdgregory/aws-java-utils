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

package com.kdgregory.aws.utils;

import org.junit.Test;
import static org.junit.Assert.*;


public class TestCommonUtils
{
    @Test
    public void testSleepQuietly() throws Exception
    {
        long start = System.currentTimeMillis();
        boolean result = CommonUtils.sleepQuietly(100);
        long elapsed = System.currentTimeMillis() - start;

        assertFalse("not interrupted", result);
        assertTrue("slept ~= 100 ms (was: " + elapsed + ")", elapsed > 95);
        assertTrue("slept ~= 100 ms (was: " + elapsed + ")", elapsed < 105);
    }


    @Test
    public void testSleepQuietlyWithInterruption() throws Exception
    {
        final Thread mainThread = Thread.currentThread();
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    Thread.sleep(100);
                    mainThread.interrupt();
                }
                catch (Exception ignored)
                {
                    // nothing here, shouldn't happen
                }
            }
        }).start();

        long start = System.currentTimeMillis();
        boolean result = CommonUtils.sleepQuietly(200);
        long elapsed = System.currentTimeMillis() - start;

        // note: range for elapsed is wider because it has to account for inter-thread scheduling

        assertTrue("was interrupted", result);
        assertTrue("slept ~= 100 ms (was: " + elapsed + ")", elapsed > 80);
        assertTrue("slept ~= 100 ms (was: " + elapsed + ")", elapsed < 120);
    }
}
