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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import net.sf.kdgcommons.util.Counters;

import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.cloudwatch.model.*;

import com.kdgregory.aws.utils.cloudwatch.MetricReporter.LoggingLevel;
import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAmazonCloudWatch;


public class TestMetricReporter
{
    private final static String DEFAULT_NAMESPACE = TestMetricReporter.class.getName();

    private Log4JCapturingAppender logCapture;

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        logCapture = Log4JCapturingAppender.getInstance();
        logCapture.reset();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testBasicOperation() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE)
                                  .withDimension("foo", "bar")
                                  .withDimension("argle", "bargle");

        final long now = System.currentTimeMillis();

        reporter.report("example", 123.45);

        mock.assertInvocationCount("putMetricData", 1);

        PutMetricDataRequest lastRequest = mock.getLastPutRequest();

        assertEquals("namespace",                       DEFAULT_NAMESPACE,              lastRequest.getNamespace());
        assertEquals("number of datums",                1,                              lastRequest.getMetricData().size());

        MetricDatum datum = lastRequest.getMetricData().get(0);

        assertEquals("metric name",                     "example",                      datum.getMetricName());
        assertInRange("metric timestamp",               now, now + 50,                  datum.getTimestamp().getTime());
        assertEquals("metric resolution",               60,                             datum.getStorageResolution().intValue());
        assertEquals("metric unit",                     StandardUnit.Count.toString(),  datum.getUnit());
        assertEquals("metric value",                    123.45,                         datum.getValue().doubleValue(), 0.0);

        // note: for testing we know the order that dimensions will be emitted; application code does not

        assertEquals("dimension 0 name",                "argle",                        datum.getDimensions().get(0).getName());
        assertEquals("dimension 0 value",               "bargle",                       datum.getDimensions().get(0).getValue());
        assertEquals("dimension 1 name",                "foo",                          datum.getDimensions().get(1).getName());
        assertEquals("dimension 1 value",               "bar",                          datum.getDimensions().get(1).getValue());

        logCapture.assertLogSize(0);
    }


    @Test
    public void testDimensionsAtReport() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE)
                                  .withDimension("argle", "bargle");

        long now = System.currentTimeMillis();

        Map<String,String> reportDimensions = new HashMap<String,String>();
        reportDimensions.put("foo", "bar");
        reporter.report("example", 123.45, reportDimensions);

        mock.assertInvocationCount("putMetricData", 1);

        PutMetricDataRequest lastRequest = mock.getLastPutRequest();

        assertEquals("namespace",                       DEFAULT_NAMESPACE,              lastRequest.getNamespace());
        assertEquals("number of datums",                1,                              lastRequest.getMetricData().size());

        MetricDatum datum = lastRequest.getMetricData().get(0);

        assertEquals("metric name",                     "example",                      datum.getMetricName());
        assertInRange("metric timestamp",               now, now + 50,                  datum.getTimestamp().getTime());
        assertEquals("metric resolution",               60,                             datum.getStorageResolution().intValue());
        assertEquals("metric unit",                     StandardUnit.Count.toString(),  datum.getUnit());
        assertEquals("metric value",                    123.45,                         datum.getValue().doubleValue(), 0.0);

        assertEquals("dimension 0 name",                "argle",                        datum.getDimensions().get(0).getName());
        assertEquals("dimension 0 value",               "bargle",                       datum.getDimensions().get(0).getValue());
        assertEquals("dimension 1 name",                "foo",                          datum.getDimensions().get(1).getName());
        assertEquals("dimension 1 value",               "bar",                          datum.getDimensions().get(1).getValue());

        logCapture.assertLogSize(0);
    }


    @Test
    public void testHighResolution() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE)
                                  .withHighResolution(true);

        reporter.report("example", 123);

        MetricDatum firstDatum = mock.getDatumFromLastPut(0);

        assertEquals("metric name",             "example",              firstDatum.getMetricName());
        assertEquals("metric resolution",       1,                      firstDatum.getStorageResolution().intValue());
        assertEquals("metric value",            123,                    firstDatum.getValue().doubleValue(), 0.0);

        reporter.withHighResolution(false);

        reporter.report("example", 456);

        MetricDatum secondDatum = mock.getDatumFromLastPut(0);

        assertEquals("metric name",             "example",              secondDatum.getMetricName());
        assertEquals("metric resolution",       60,                     secondDatum.getStorageResolution().intValue());
        assertEquals("metric value",            456,                    secondDatum.getValue().doubleValue(), 0.0);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testAlternateUnits() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE)
                                  .withUnit(StandardUnit.Bytes);

        reporter.report("example", 123);

        MetricDatum datum = mock.getDatumFromLastPut(0);

        assertEquals("metric name",             "example",                      datum.getMetricName());
        assertEquals("metric unit",             StandardUnit.Bytes.toString(),  datum.getUnit());
        assertEquals("metric value",            123,                            datum.getValue().doubleValue(), 0.0);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testMultipleMetricsOneBatch() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE);

        reporter.add("foo", 123);
        reporter.add("bar", 456);

        mock.assertInvocationCount("before flush", "putMetricData", 0);

        reporter.flush();

        mock.assertInvocationCount("after flush", "putMetricData", 1);

        List<MetricDatum> metricData = mock.getLastPutRequest().getMetricData();

        assertEquals("metrics in batch",        2,                              metricData.size());

        assertEquals("first metric, name",      "foo",                          metricData.get(0).getMetricName());
        assertEquals("first metric, value",     123,                            metricData.get(0).getValue().doubleValue(), 0.0);

        assertEquals("second metric, name",     "bar",                          metricData.get(1).getMetricName());
        assertEquals("second metric, value",    456,                            metricData.get(1).getValue().doubleValue(), 0.0);

        logCapture.assertLogSize(0);
    }


    @Test
    public void testMultipleBatches() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE);

        for (int ii = 0 ; ii < 30 ; ii++)
        {
            reporter.add(String.format("metric-%02d", ii), ii);
        }
        reporter.flush();

        mock.assertInvocationCount("putMetricData", 2);

        List<MetricDatum> batch0 = mock.getDataFromSavedPut(0);
        List<MetricDatum> batch1 = mock.getDataFromSavedPut(1);

        assertEquals("batch 0 size",            20,                             batch0.size());
        assertEquals("batch 1 size",            10,                             batch1.size());

        assertEquals("batch 0, first metric",   "metric-00",                    batch0.get(0).getMetricName());
        assertEquals("batch 0, last metric",    "metric-19",                    batch0.get(19).getMetricName());

        assertEquals("batch 1, first metric",   "metric-20",                    batch1.get(0).getMetricName());
        assertEquals("batch 1, last metric",    "metric-29",                    batch1.get(9).getMetricName());

        logCapture.assertLogSize(0);
    }


    @Test
    public void testExceptionInPut() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch()
        {
            @Override
            public PutMetricDataResult putMetricData(PutMetricDataRequest request)
            {
                // we'll record the request to verify that we were called
                super.putMetricData(request);
                throw new IllegalArgumentException("bogus!");
            }
        };

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE);

        reporter.add("example", 123.45);
        reporter.flush();

        mock.assertInvocationCount("after first flush", "putMetricData", 1);

        reporter.flush();

        mock.assertInvocationCount("after second flush", "putMetricData", 1);

        logCapture.assertLogEntry(0, Level.WARN, "failed to publish 1.*" + DEFAULT_NAMESPACE + ".*");
    }


    @Test
    public void testBatchLogging() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE);

        reporter.withBatchLogging(LoggingLevel.MINIMAL);

        // empty flush should still report
        reporter.flush();
        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, DEFAULT_NAMESPACE + ": flush called with empty queue");
        logCapture.reset();

        reporter.report("foo", 123);
        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, DEFAULT_NAMESPACE + ": flush sending 1 metric");
        logCapture.reset();

        reporter.add("foo", 123);
        reporter.add("bar", 456);
        reporter.flush();
        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, DEFAULT_NAMESPACE + ": flush sending 2 metrics");
        logCapture.reset();

        reporter.withBatchLogging(LoggingLevel.NAMES);

        reporter.add("foo", 123);
        reporter.add("bar", 456);
        reporter.flush();
        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, DEFAULT_NAMESPACE + ": flush sending 2 metrics: foo, bar");
        logCapture.reset();

        reporter.withBatchLogging(LoggingLevel.DETAILED);

        reporter.add("foo", 123);
        reporter.add("bar", 456);
        reporter.flush();
        logCapture.assertLogSize(1);
        logCapture.assertLogEntry(0, Level.DEBUG, DEFAULT_NAMESPACE + ": flush sending 2 metrics:\n"
                                                  + "    foo: 123.0 " + StandardUnit.Count + " @ 60\n"
                                                  + "    bar: 456.0 " + StandardUnit.Count + " @ 60");
        logCapture.reset();
    }


    @Test
    public void testAsynchronousOperation() throws Exception
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        long interval = 150;

        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();
        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE, executor, interval)
                                  .withBatchLogging(LoggingLevel.NAMES);

        // these should definitely exectute before the background thread does

        reporter.add("foo", 123);
        reporter.add("bar", 456);

        // easiest just to sleep until background thread invoked
        Thread.sleep(interval * 2 + 10);

        mock.assertInvocationCount("after first sleep", "putMetricData", 1);
        mock.assertLastInvocationNotOnCurrentThread("putMetricData");

        PutMetricDataRequest lastRequest = mock.getLastPutRequest();

        assertEquals("namespace",                           DEFAULT_NAMESPACE,  lastRequest.getNamespace());
        assertEquals("number of datums",                    2,                  lastRequest.getMetricData().size());
        assertEquals("datum 0 name",                        "foo",              lastRequest.getMetricData().get(0).getMetricName());
        assertEquals("datum 1 name",                        "bar",              lastRequest.getMetricData().get(1).getMetricName());

        // another sleep: once the queue is clear there shouldn't be another send
        Thread.sleep(interval * 2);

        mock.assertInvocationCount("after second sleep", "putMetricData", 1);

        // we'll use the log to determine that we were called multiple times ... depending on threads
        // and scheduling, we may not have an exact count

        Counters<String> messageCounters = new Counters<String>();
        for (LoggingEvent logEvent : logCapture.events)
        {
            messageCounters.increment(logEvent.getMessage().toString());
        }

        for (String message : messageCounters.keySet())
        {
            if (message.contains("flush called with empty queue"))
                assertTrue("empty flush called more than once", messageCounters.get(message).intValue() > 2);
            if (message.contains("foo, bar"))
                assertEquals("flush called only once with messages", 1, messageCounters.get(message).intValue());
        }

        executor.shutdownNow();
    }


    @Test
    public void testAsynchronousOperationExceptionInPut() throws Exception
    {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        long interval = 150;

        MockAmazonCloudWatch mock = new MockAmazonCloudWatch()
        {
            @Override
            public PutMetricDataResult putMetricData(PutMetricDataRequest request)
            {
                throw new RuntimeException("should be caught and discarded");
            }
        };
        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE, executor, interval)
                                  .withBatchLogging(LoggingLevel.NAMES);

        reporter.add("foo", 123);

        // sleep for at least 3 invocations
        Thread.sleep(interval * 3 + 20);

        mock.assertInvocationCount("putMetricData", 1);

        Counters<String> messageCounters = new Counters<String>();
        for (LoggingEvent logEvent : logCapture.events)
        {
            messageCounters.increment(logEvent.getMessage().toString());
        }

        for (String message : messageCounters.keySet())
        {
            if (message.contains("flush called with empty queue"))
                assertTrue("empty flush should be called multiple times", messageCounters.get(message).intValue() > 1);
            if (message.contains("failed to publish"))
                assertEquals("exception should only be reported once", 1, messageCounters.get(message).intValue());
        }

        executor.shutdownNow();
    }
}
