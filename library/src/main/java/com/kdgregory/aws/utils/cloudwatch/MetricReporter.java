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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;

/**
 *  Reports CloudWatch metrics. Each instance reports metrics for a single namespace,
 *  and may be configured with default dimensions for each report (for example, EC2
 *  instance ID). Individual calls to {@link #report} specify a metric name and
 *  (optional) additional dimensions.
 *  <p>
 *  Reporting can either take place inline or on a (provided) background thread.
 *  Most applications should choose the background thread, to avoid processing
 *  lags due to AWS service calls.
 */
public class MetricReporter
{
    private Log logger = LogFactory.getLog(getClass());

    private Executor executor;
    private AmazonCloudWatch client;
    private String namespace;
    private Map<String,String> defaultDimensions = new TreeMap<String,String>();
    private Integer storageResolution = Integer.valueOf(60);
    private StandardUnit unit = StandardUnit.Count;


    /**
     *  Base constructor.
     *
     *  @param executor     Used to execute service calls. This should be an application-managed
     *                      threadpool with a small number of background threads.
     *  @param client       The service client. AWS best practice is to create a single client
     *                      that is then shared between all consumers.
     *  @param namespace    The namespace for metrics reported by this instance.
     *  @param dimensions   Default dimensions to apply to all reports.
     */
    public MetricReporter(Executor executor, AmazonCloudWatch client, String namespace, Map<String,String> dimensions)
    {
        this.executor = executor;
        this.client = client;
        this.namespace = namespace;
        this.defaultDimensions.putAll(dimensions);
    }


    /**
     *  Convenience constructor, which has no default dimensions. This is used when you are
     *  configuring reporters in a "fluent" style.
     *
     *  @param executor     Used to execute service calls. This should be an application-managed
     *                      threadpool with a small number of background threads.
     *  @param client       The service client. AWS best practice is to create a single client
     *                      that is then shared between all consumers.
     *  @param namespace    The namespace for metrics reported by this instance.
     */
    public MetricReporter(Executor executor, AmazonCloudWatch client, String namespace)
    {
        this(executor, client, namespace, Collections.<String,String>emptyMap());
    }


    /**
     *  Creates an instance that reports metrics synchonously, with default dimensions. This
     *  variant is intended to support the use case where a single set of dimensions may be
     *  shared between different metrics.
     *
     *  @param client       The service client. AWS best practice is to create a single client
     *                      that is then shared between all consumers.
     *  @param namespace    The namespace for metrics reported by this instance.
     *  @param dimensions   Default dimensions to apply to all reports.
     */
    public MetricReporter(AmazonCloudWatch client, String namespace, Map<String,String> dimensions)
    {
        this(new InlineExecutor(), client, namespace, dimensions);
    }


    /**
     *  Creates an instance that reports metrics synchonously.
     *
     *  @param client       The service client. AWS best practice is to create a single client
     *                      that is then shared between all consumers.
     *  @param namespace    The namespace for metrics reported by this instance.
     */
    public MetricReporter(AmazonCloudWatch client, String namespace)
    {
        this(client, namespace, Collections.<String,String>emptyMap());
    }

//----------------------------------------------------------------------------
//  Post-construction configuration
//----------------------------------------------------------------------------

    /**
     *  Adds a default dimension to reported metrics.
     */
    public MetricReporter withDimension(String name, String value)
    {
        defaultDimensions.put(name, value);
        return this;
    }


    /**
     *  Sets the units that this metric reports. Default is COUNT.
     */
    public MetricReporter withUnit(StandardUnit value)
    {
        unit = value;
        return this;
    }


    /**
     *  Switches this reporter to high resolution (or back to standard).
     */
    public MetricReporter withHighResolution(boolean value)
    {
        storageResolution = value ? Integer.valueOf(1) : Integer.valueOf(60);
        return this;
    }

//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  Reports a metric value, using the current timestamp and default dimensions.
     */
    public void report(String metricName, double value)
    {
        report(metricName, System.currentTimeMillis(), Collections.<String,String>emptyMap(), value);
    }


    /**
     *  Reports a metric value, using the current timestamp and specified dimensions.
     */
    public void report(String metricName, Map<String,String> dimensions, double value)
    {
        report(metricName, System.currentTimeMillis(), dimensions, value);
    }


    /**
     *  Reports this metric value, using the specified timestamp and default dimensions.
     */
    public void report(String metricName, long timestamp, double value)
    {
        report(metricName, timestamp, Collections.<String,String>emptyMap(), value);
    }


    /**
     *  Reports this metric value, using the specified timestamp and dimensions. If the
     *  report-time dimensions have the same names as the default dimensions, they will
     *  override the default.
     */
    public void report(String metricName, long timestamp, Map<String,String> dimensions, double value)
    {
        MetricDatum datum = new MetricDatum()
                            .withMetricName(metricName)
                            .withTimestamp(new Date(timestamp))
                            .withDimensions(createDimensionsList(dimensions))
                            .withStorageResolution(storageResolution)
                            .withUnit(unit)
                            .withValue(value);
        putMetric(datum);
    }


//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  This simplifies the code: no need for a null check.
     */
    private static class InlineExecutor
    implements Executor
    {
        @Override
        public void execute(Runnable command)
        {
            command.run();
        }
    }


    /**
     *  Combines default dimensions with report-time dimensions and turns the result
     *  into a list.
     */
    private List<Dimension> createDimensionsList(Map<String,String> reportDimensions)
    {
        Map<String,String> combinedDimensions = new TreeMap<String,String>(defaultDimensions);
        combinedDimensions.putAll(reportDimensions);

        List<Dimension> result = new ArrayList<Dimension>();
        for (Map.Entry<String,String> entry : combinedDimensions.entrySet())
        {
            result.add(new Dimension().withName(entry.getKey()).withValue(entry.getValue()));
        }

        return result;
    }


    /**
     *  Creates the actual request and sends it off.
     */
    private void putMetric(final MetricDatum datum)
    {
        executor.execute(new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    PutMetricDataRequest request = new PutMetricDataRequest()
                                        .withNamespace(namespace)
                                        .withMetricData(Arrays.asList(datum));
                    client.putMetricData(request);
                }
                catch (Exception ex)
                {
                    logger.warn("failed to publish metric \"" + datum.getMetricName() + "\" in namespace \"" + namespace + "\"", ex);
                }
            }
        });
    }
}
