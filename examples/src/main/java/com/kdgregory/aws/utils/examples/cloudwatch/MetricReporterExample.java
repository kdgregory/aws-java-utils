// Copyright (c) Keith D Gregory, all rights reserved
package com.kdgregory.aws.utils.examples.cloudwatch;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import net.sf.kdgcommons.lang.ThreadUtil;

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
        Map<String,String> defaultDimensions = new HashMap<String,String>();
        defaultDimensions.put("environment", "example");

        final MetricReporter reporter = new MetricReporter("com.kdgregory.aws.utils.examples.cloudwatch", defaultDimensions);

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
                        reporter.report("randomWalk", threadDimensions, value);
                        ThreadUtil.sleepQuietly(10000L);
                    }
                }
            }).start();
        }

        System.out.println("main thread sleeping for 15 minutes");
        Thread.sleep(15 * 60000);
    }
}
