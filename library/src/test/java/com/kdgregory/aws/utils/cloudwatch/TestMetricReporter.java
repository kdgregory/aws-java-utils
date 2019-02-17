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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;

import net.sf.kdgcommons.test.SelfMock;
import static net.sf.kdgcommons.test.NumericAsserts.*;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;

import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;


public class TestMetricReporter
{
    private final static String DEFAULT_NAMESPACE = TestMetricReporter.class.getName();

    /**
     *  A mock that records its invocations. Override to cause errors.
     */
    private static class MockAmazonCloudWatch
    extends SelfMock<AmazonCloudWatch>
    {
        public volatile PutMetricDataRequest lastPutMetricDataRequest;
        public volatile Thread executedOn;

        public MockAmazonCloudWatch()
        {
            super(AmazonCloudWatch.class);
        }

        @SuppressWarnings("unused")
        public PutMetricDataResult putMetricData(PutMetricDataRequest request)
        {
            lastPutMetricDataRequest = request;
            executedOn = Thread.currentThread();
            return new PutMetricDataResult();
        }
    }

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        Log4JCapturingAppender.getInstance().reset();
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

        assertNotNull("putMetricData() called",                                 mock.lastPutMetricDataRequest);
        assertEquals("namespace",               DEFAULT_NAMESPACE,              mock.lastPutMetricDataRequest.getNamespace());
        assertEquals("number of datums",        1,                              mock.lastPutMetricDataRequest.getMetricData().size());

        MetricDatum datum = mock.lastPutMetricDataRequest.getMetricData().get(0);

        assertEquals("metric name",             "example",                      datum.getMetricName());
        assertInRange("metric timestamp",       now, now + 50,                  datum.getTimestamp().getTime());
        assertEquals("metric resolution",       60,                             datum.getStorageResolution().intValue());
        assertEquals("metric unit",             StandardUnit.Count.toString(),  datum.getUnit());
        assertEquals("metric value",            123.45,                         datum.getValue().doubleValue(), 0.0);

        // note: for testing we know the order that dimensions will be emitted; application code does not

        assertEquals("dimension 0 name",        "argle",                        datum.getDimensions().get(0).getName());
        assertEquals("dimension 0 value",       "bargle",                       datum.getDimensions().get(0).getValue());
        assertEquals("dimension 1 name",        "foo",                          datum.getDimensions().get(1).getName());
        assertEquals("dimension 1 value",       "bar",                          datum.getDimensions().get(1).getValue());
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
        reporter.report("example", reportDimensions, 123.45);

        assertNotNull("putMetricData() called",                                 mock.lastPutMetricDataRequest);
        assertEquals("namespace",               DEFAULT_NAMESPACE,              mock.lastPutMetricDataRequest.getNamespace());
        assertEquals("number of datums",        1,                              mock.lastPutMetricDataRequest.getMetricData().size());

        MetricDatum datum = mock.lastPutMetricDataRequest.getMetricData().get(0);

        assertEquals("metric name",             "example",                      datum.getMetricName());
        assertInRange("metric timestamp",       now, now + 50,                  datum.getTimestamp().getTime());
        assertEquals("metric resolution",       60,                             datum.getStorageResolution().intValue());
        assertEquals("metric unit",             StandardUnit.Count.toString(),  datum.getUnit());
        assertEquals("metric value",            123.45,                         datum.getValue().doubleValue(), 0.0);

        assertEquals("dimension 0 name",        "argle",                        datum.getDimensions().get(0).getName());
        assertEquals("dimension 0 value",       "bargle",                       datum.getDimensions().get(0).getValue());
        assertEquals("dimension 1 name",        "foo",                          datum.getDimensions().get(1).getName());
        assertEquals("dimension 1 value",       "bar",                          datum.getDimensions().get(1).getValue());
    }


    @Test
    public void testHighResolution() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE)
                                  .withHighResolution(true);

        reporter.report("example", 123);

        MetricDatum firstDatum = mock.lastPutMetricDataRequest.getMetricData().get(0);

        assertEquals("metric name",             "example",              firstDatum.getMetricName());
        assertEquals("metric resolution",       1,                      firstDatum.getStorageResolution().intValue());
        assertEquals("metric value",            123,                    firstDatum.getValue().doubleValue(), 0.0);

        reporter.withHighResolution(false);

        reporter.report("example", 456);

        MetricDatum secondDatum = mock.lastPutMetricDataRequest.getMetricData().get(0);

        assertEquals("metric name",             "example",              secondDatum.getMetricName());
        assertEquals("metric resolution",       60,                     secondDatum.getStorageResolution().intValue());
        assertEquals("metric value",            456,                    secondDatum.getValue().doubleValue(), 0.0);
    }


    @Test
    public void testAlternateUnits() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        MetricReporter reporter = new MetricReporter(mock.getInstance(), DEFAULT_NAMESPACE)
                                  .withUnit(StandardUnit.Bytes);

        reporter.report("example", 123);

        MetricDatum firstDatum = mock.lastPutMetricDataRequest.getMetricData().get(0);

        assertEquals("metric name",             "example",                      firstDatum.getMetricName());
        assertEquals("metric resolution",       StandardUnit.Bytes.toString(),  firstDatum.getUnit());
        assertEquals("metric value",            123,                            firstDatum.getValue().doubleValue(), 0.0);
    }


    @Test
    public void testAsynchronousOperation() throws Exception
    {
        MockAmazonCloudWatch mock = new MockAmazonCloudWatch();

        ExecutorService executor = Executors.newSingleThreadExecutor();

        MetricReporter reporter = new MetricReporter(executor, mock.getInstance(), DEFAULT_NAMESPACE);

        reporter.report("example", 123);

        // rather than use semaphores to coordinate test thread and background thread, we'll just spin
        for (int ii = 0 ; ii < 10 ; ii++)
        {
            if (mock.executedOn != null)
                break;
            Thread.sleep(50);
        }

        assertTrue("putMetricData not called on main thread",           mock.executedOn != Thread.currentThread());

        assertNotNull("putMetricData() called",                         mock.lastPutMetricDataRequest);
        assertEquals("namespace",               DEFAULT_NAMESPACE,      mock.lastPutMetricDataRequest.getNamespace());
        assertEquals("number of datums",        1,                      mock.lastPutMetricDataRequest.getMetricData().size());

        executor.shutdown();

        try
        {
            reporter.report("example", 456);
            fail("was able to submit after executor shut down");
        }
        catch (RejectedExecutionException ex)
        {
            // success
        }
    }


    @Test
    public void testExceptionInPutMetricData() throws Exception
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

        reporter.report("example", 123.45);

        assertNotNull("putMetricData() called", mock.lastPutMetricDataRequest);

        Log4JCapturingAppender.getInstance().assertContent(
            "failed to publish.*example.*" + DEFAULT_NAMESPACE + ".*",
            0);
    }
}
