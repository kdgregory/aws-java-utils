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

import java.io.UnsupportedEncodingException;

/**
 *  Static utility methods that cross service boundaries.
 */
public class CommonUtils
{

    /**
     *  Sleeps for the specified amount of time unless interrupted.
     *
     *  @param  sleepTime   Number of milliseconds to sleep.
     *
     *  @return true if the sleep was interrupted, false otherwise.
     */
    public static boolean sleepQuietly(long sleepTime)
    {
        try
        {
            Thread.sleep(sleepTime);
            return false;
        }
        catch (InterruptedException ex)
        {
            return true;
        }
    }


    /**
     *  Converts a string to its UTF-8 representation. Replaces the checked exception
     *  with a runtime exception that should never happen unless the JVM installation
     *  is completely borked.
     */
    public static byte[] toUTF8(String str)
    {
        try
        {
            return str.getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException ex)
        {
            throw new RuntimeException("JVM does not support UTF-8", ex);
        }
    }

}
