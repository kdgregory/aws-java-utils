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

}
