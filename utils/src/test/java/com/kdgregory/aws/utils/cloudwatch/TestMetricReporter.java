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

package com.kdgregory.aws.utils.cloudwatch;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.test.NumericAsserts;
import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;


/**
 *  Performs mock-object tests of MetricReporter.
 */
public class TestMetricReporter
{
    private static class Mock extends SelfMock<AmazonCloudWatch>
    {
        public List<PutMetricDataRequest> requests = Collections.synchronizedList(new ArrayList<PutMetricDataRequest>());

        public Mock()
        {
            super(AmazonCloudWatch.class);
        }

        @SuppressWarnings("unused")
        public PutMetricDataResult putMetricData(PutMetricDataRequest request)
        {
            requests.add(request);
            return new PutMetricDataResult();
        }

        /**
         *  Blocks the test thread until either the specified time elapses or the
         *  background thread makes the expected number of requests.
         */
        public void waitForBackgroundThread(int numRequests, long millisToWait)
        {
            long stopWaitingAt = System.currentTimeMillis() + millisToWait;
            while ((System.currentTimeMillis() <= stopWaitingAt) && (requests.size() < numRequests))
            {
                try
                {
                    Thread.sleep(50);
                }
                catch (InterruptedException ignored) { /* */ }
            }
        }
    }

//----------------------------------------------------------------------------
//  Test values
//----------------------------------------------------------------------------

    private final static String NAMESPACE = "com.kdgregory.aws.utils.cloudwatch.test";

    private final static String METRIC_NAME_1 = "metric-1";

    private final static String DEFAULT_DIMENSION_NAME = "defaultName";
    private final static String DEFAULT_DIMENSION_VALUE = "defaultValue";

    private final static String EXPLICIT_DIMENSION_NAME = "explicitName";
    private final static String EXPLICIT_DIMENSION_VALUE = "explicitValue";

    private final static Map<String,String> NO_DIMENSIONS = Collections.emptyMap();

    private final static double VALUE_1 = 123;


//----------------------------------------------------------------------------
//  Configuration
//----------------------------------------------------------------------------

    private StringWriter logOutput = new StringWriter();

    @Before
    public void setUp() throws Exception
    {
        Logger rootLogger = LogManager.getRootLogger();
        WriterAppender appender = (WriterAppender)rootLogger.getAppender("default");
        appender.setWriter(logOutput);

    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testSimpleOperation() throws Exception
    {
        Map<String,String> defaultDimensions = new HashMap<String,String>();
        defaultDimensions.put(DEFAULT_DIMENSION_NAME, DEFAULT_DIMENSION_VALUE);

        Map<String,String> explicitDimensions = new HashMap<String,String>();
        explicitDimensions.put(EXPLICIT_DIMENSION_NAME, EXPLICIT_DIMENSION_VALUE);

        Mock mock = new Mock();
        MetricReporter reporter = new MetricReporter(
                                        mock.getInstance(),
                                        Executors.newSingleThreadExecutor(),
                                        NAMESPACE,
                                        defaultDimensions);

        reporter.report(METRIC_NAME_1, explicitDimensions, VALUE_1);
        mock.waitForBackgroundThread(1, 250);

        PutMetricDataRequest request = mock.requests.get(0);
        assertEquals("namespace", NAMESPACE, request.getNamespace());

        List<MetricDatum> metrics = request.getMetricData();
        assertEquals("number of metrics reported", 1, metrics.size());

        MetricDatum metric = metrics.get(0);
        assertEquals("metric name",     METRIC_NAME_1,                  metric.getMetricName());
        assertEquals("metric value",    VALUE_1,                        metric.getValue().doubleValue(), 0);
        assertEquals("metric unit",     StandardUnit.Count.toString(),  metric.getUnit());

        long now = System.currentTimeMillis();
        NumericAsserts.assertInRange(now - 1000, now, metric.getTimestamp().getTime());

        Map<String,String> dimensions = new HashMap<String,String>();
        for (Dimension dimension : metric.getDimensions())
        {
            dimensions.put(dimension.getName(), dimension.getValue());
        }

        assertEquals("number of dimensions", 2, dimensions.size());
        assertEquals("default dimension: " + DEFAULT_DIMENSION_NAME, DEFAULT_DIMENSION_VALUE, dimensions.get(DEFAULT_DIMENSION_NAME));
        assertEquals("explicit dimension: " + EXPLICIT_DIMENSION_NAME, EXPLICIT_DIMENSION_VALUE, dimensions.get(EXPLICIT_DIMENSION_NAME));
    }


    @Test
    public void testExeptionHandling() throws Exception
    {
        Mock mock = new Mock()
        {
            @Override
            public PutMetricDataResult putMetricData(PutMetricDataRequest request)
            {
                throw new InternalServiceException("fail!");
            }
        };

        MetricReporter reporter = new MetricReporter(
                                        mock.getInstance(),
                                        Executors.newSingleThreadExecutor(),
                                        NAMESPACE,
                                        NO_DIMENSIONS);

        reporter.report(METRIC_NAME_1, NO_DIMENSIONS, VALUE_1);

        // we can't use the semaphore to wait for the background thread, so will spin on the logger
        String logMessage = null;
        for (int ii = 0 ; ii < 10 ; ii++)
        {
            logMessage = logOutput.toString();
            if (! logMessage.isEmpty())
                break;
            else
                Thread.sleep(25);
        }

        assertTrue("log message is ERROR",              logMessage.startsWith("ERROR"));
        assertTrue("log message identifies reporter",   logMessage.contains(MetricReporter.class.getName()));
        assertTrue("log message identifies metric",     logMessage.contains(NAMESPACE));
        assertTrue("log message identifies exception",  logMessage.contains(InternalServiceException.class.getName()));
    }


    @Test
    public void testBatching() throws Exception
    {
        Mock mock = new Mock();
        MetricReporter reporter = new MetricReporter(
                                        mock.getInstance(),
                                        Executors.newSingleThreadExecutor(),
                                        NAMESPACE,
                                        NO_DIMENSIONS,
                                        4,
                                        200);

        long start = System.currentTimeMillis();
        for (int ii = 0 ; ii < 6 ; ii++)
        {
            reporter.report(METRIC_NAME_1, NO_DIMENSIONS, ii);
        }
        mock.waitForBackgroundThread(2, 250);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("number of requests", 2, mock.requests.size());

        // first batch should take minimal amount of time, but we had to wait for second
        NumericAsserts.assertInRange("elapsed time", 200, 275, elapsed);

        assertEquals("number of metrics in batch 1", 4, mock.requests.get(0).getMetricData().size());
        assertEquals("number of metrics in batch 2", 2, mock.requests.get(1).getMetricData().size());

        List<MetricDatum> combinedData = CollectionUtil.combine(
                                            new ArrayList<MetricDatum>(),
                                            mock.requests.get(0).getMetricData(),
                                            mock.requests.get(1).getMetricData());

        for (int ii = 0 ; ii < 6 ; ii++)
        {
            assertEquals("metric " + ii + " value", ii, combinedData.get(ii).getValue(), 0);
        }
    }
}
