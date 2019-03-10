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

package com.kdgregory.aws.utils.examples.cloudwatch;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import net.sf.kdgcommons.lang.ThreadUtil;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;

import com.kdgregory.aws.utils.cloudwatch.MetricReporter;


/**
 *  Demonstrates the use of the metric reporter. Note that you will be charged
 *  for each dimension.
 */
public class MetricReporterExample
{
    public static void main(String[] argv)
    throws Exception
    {
        final AmazonCloudWatch client = AmazonCloudWatchClientBuilder.defaultClient();
        final MetricReporter reporter = new MetricReporter(client, "com.kdgregory.aws.utils.examples.cloudwatch")
                                        .withDimension("environment", "example");

        for (int ii = 0 ; ii< 2 ; ii++)
        {
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    Map<String,String> threadDimensions = new HashMap<String,String>();
                    threadDimensions.put("threadName", Thread.currentThread().getName());

                    Random rnd = new Random(Thread.currentThread().getId());
                    int value = 1000;

                    while (true)
                    {
                        value += rnd.nextInt(101) - 50;
                        System.out.println(Thread.currentThread().getName() + ": " + value);
                        reporter.report("example", value, threadDimensions);
                        ThreadUtil.sleepQuietly(10000L);
                    }
                }
            }).start();
        }

        System.out.println("main thread sleeping for 15 minutes");
        Thread.sleep(15 * 60000);
    }
}
