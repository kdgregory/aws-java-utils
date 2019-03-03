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
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;


/**
 *  Reports CloudWatch metrics for a single namespace.
 *  <p>
 *  By default, metrics are reported as "Count", in normal resolution, and with no
 *  dimensions. The reporter may be configured with default values for all of these
 *  items, and individual metrics may be reported with dimensions that extend or
 *  override the defaults.
 *  <p>
 *  Call {@link #add} to add metrics to a queue for reporting, and {@link #flush} to send
 *  all queued metrics to CloudWatch. For convenience, {@link #report} will add and flush
 *  in a single step, although it supports fewer options.
 *  <p>
 *  The reporter also supports background sending of metrics on a regular basis. Construct
 *  with a <code>ScheduledExecutorService</code> and a reporting interval, and all queued
 *  metrics will be sent using that service. Background sending is generally preferred, as
 *  each flush may take 50 ms or more, but beware that metrics may be lost if the process
 *  shuts down unexpectedly (or if the executor service is shut down).
 */
public class MetricReporter
{
    /**
     *  Controls the amount of logging when sending a batch.
     */
    public enum LoggingLevel
    {
        /** Disables all logging (default) */
        NONE,

        /** Minimal logging: indicates that the batch was sent, and number of metrics */
        MINIMAL,

        /** Log message includes names of metrics */
        NAMES,

        /** Each metric is reported as a separate log entry, with name, value, units, and dimensions */
        DETAILED
    }

//----------------------------------------------------------------------------
//  Instance variables and constructors
//----------------------------------------------------------------------------

    private Log logger = LogFactory.getLog(getClass());

    private AmazonCloudWatch client;
    private String namespace;
    private Map<String,String> defaultDimensions = new TreeMap<String,String>();
    private boolean useHighResolution = false;
    private StandardUnit unit = StandardUnit.Count;

    private LinkedBlockingDeque<MetricDatum> queuedMetrics = new LinkedBlockingDeque<MetricDatum>();

    private volatile LoggingLevel batchLoggingLevel = LoggingLevel.NONE;


    /**
     *  Constructs an instance instance that reports metrics synchronously.
     *
     *  @param  client      The service client. AWS best practice is to create a single client
     *                      that is then shared between all consumers.
     *  @param  namespace   The namespace for metrics reported by this instance.
     */
    public MetricReporter(AmazonCloudWatch client, String namespace)
    {
        this.client = client;
        this.namespace = namespace;
    }


    /**
     *  Base constructor for an instance that reports metrics asynchronously. Will start
     *  reporting metrics <code>interval</code> milliseconds after construction.
     *
     *  @param  client      The service client. AWS best practice is to create a single client
     *                      that is then shared between all consumers.
     *  @param  namespace   The namespace for metrics reported by this instance.
     *  @param  executor    Shareable threadpool used to report statistics. The size of this
     *                      pool depends on how many reporters (or other scheduled async
     *                      objects) use it, but generally 1 or 2 threads is sufficient.
     *  @param  interval    Number of milliseconds between invocations of {@link #flush}.
     *                      This value represents a tradeoff between minimizing service
     *                      calls and potential for message loss on unexpected shutdown,
     *                      but 500 is often appropriate.
     */
    public MetricReporter(AmazonCloudWatch client, String namespace, ScheduledExecutorService executor, long interval)
    {
        this(client, namespace);
        executor.schedule(new ScheduledInvoker(executor, interval, this), interval, TimeUnit.MILLISECONDS);
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
        useHighResolution = value;
        return this;
    }


    /**
     *  Enables (or disables) logging of each call to <code>flush()</code>.
     *  This is useful during application development, but may be a distraction
     *  during operations. By default, successful batches do not log; failures
     *  are always logged.
     */
    public MetricReporter withBatchLogging(LoggingLevel level)
    {
        batchLoggingLevel = level;
        return this;
    }

//----------------------------------------------------------------------------
//  Public API
//----------------------------------------------------------------------------

    /**
     *  Adds a metric to the queue to be reported by the next {@link #flush}.
     *
     *  @param  metricName  The metric's name.
     *  @param  timestamp   The metric's timestamp.
     *  @param  value       The metric's value.
     *  @param  units       The metric's unit type.
     *  @param  isHiRes     If true, report as high resolution.
     *  @param  dimension   Overrides or supplements the default dimensions.
     */
    public void add(String metricName, long timestamp, Map<String,String> dimensions,
                    double value, StandardUnit units, boolean isHiRes)
    {
        Integer storageResolution = isHiRes ? Integer.valueOf(1) : Integer.valueOf(60);
        MetricDatum datum = new MetricDatum()
                            .withMetricName(metricName)
                            .withTimestamp(new Date(timestamp))
                            .withDimensions(createDimensionsList(dimensions))
                            .withStorageResolution(storageResolution)
                            .withUnit(unit)
                            .withValue(value);
        queuedMetrics.add(datum);
    }


    /**
     *  Adds a metric to the queue to be reported by the next {@link #flush}.
     *  This variant uses the current timestamp and default dimensions, units,
     *  and storage resolution
     */
    public void add(String metricName, double value)
    {
        add(metricName, System.currentTimeMillis(), Collections.<String,String>emptyMap(), value, unit, useHighResolution);
    }


    /**
     *  Adds a metric to the queue to be reported by the next {@link #flush}.
     *  This variant uses the current timestamp and default units and storage
     *  resolution, but allows overriding dimensions.
     */
    public void add(String metricName, double value, Map<String,String> dimensions)
    {
        add(metricName, System.currentTimeMillis(), dimensions, value, unit, useHighResolution);
    }


    /**
     *  Adds a metric to the queue to be reported by the next {@link #flush}.
     *  This variant uses the current timestamp and default storage resolution,
     *  but allows overriding units and dimensions.
     */
    public void add(String metricName, double value, StandardUnit units, Map<String,String> dimensions)
    {
        add(metricName, System.currentTimeMillis(), dimensions, value, unit, useHighResolution);
    }


    /**
     *  Flushes the current list of queued metrics. Makes a best attempt to
     *  clear the queue, breaking it into as many separate SDK calls as
     *  needed. Will log a warning and leave messages in the queu if unable
     *  to send.
     */
    public void flush()
    {
        if (queuedMetrics.isEmpty())
        {
            if (batchLoggingLevel != LoggingLevel.NONE)
            {
                logger.debug(namespace + ": flush called with empty queue");
            }
            return;
        }

        while (! queuedMetrics.isEmpty())
        {
            List<MetricDatum> batch = buildBatch();
            sendBatch(batch);
        }
    }


    /**
     *  Convenience method that adds a metric to the queue and immediately calls
     *  {@link #flush}.
     */
    public void report(String metricName, double value)
    {
        add(metricName, value);
        flush();
    }


    /**
     *  Convenience method that adds a metric to the queue and immediately calls
     *  {@link #flush}.
     */
    public void report(String metricName, double value, Map<String,String> dimensions)
    {
        add(metricName, value, dimensions);
        flush();
    }


    /**
     *  Convenience method that adds a metric to the queue and immediately calls
     *  {@link #flush}.
     */
    public void report(String metricName, double value, StandardUnit units, Map<String,String> dimensions)
    {
        add(metricName, value, units, dimensions);
        flush();
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Instances of this class are passed to the executor.
     */
    private static class ScheduledInvoker
    implements Runnable
    {
        private ScheduledExecutorService executor;
        private long interval;
        private MetricReporter reporter;

        public ScheduledInvoker(ScheduledExecutorService executor, long interval, MetricReporter reporter)
        {
            this.executor = executor;
            this.interval = interval;
            this.reporter = reporter;
        }

        @Override
        public void run()
        {
            reporter.flush();
            executor.schedule(this, interval, TimeUnit.MILLISECONDS);
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


    private List<MetricDatum> buildBatch()
    {
        List<MetricDatum> batch = new ArrayList<MetricDatum>();
        Iterator<MetricDatum> queueItx = queuedMetrics.iterator();
        while (queueItx.hasNext() && (batch.size() < 20))
        {
            batch.add(queueItx.next());
            queueItx.remove();
        }
        return batch;
    }


    private void sendBatch(List<MetricDatum> batch)
    {
        if ((batchLoggingLevel != LoggingLevel.NONE) && logger.isDebugEnabled())
        {
            logger.debug(namespace + ": " + constructLogMessage(batch));
        }
        try
        {
            PutMetricDataRequest request = new PutMetricDataRequest()
                                .withNamespace(namespace)
                                .withMetricData(batch);
            client.putMetricData(request);
            batch.clear();
        }
        catch (Exception ex)
        {
            logger.warn("failed to publish " + batch.size() + " metrics for namespace " + namespace
                        + ": " + ex.getMessage());
            // failures are generally unrecoverable, so discard and move on
            batch.clear();
        }
    }


    /**
     *  Constructs a logging message that depends on log level.
     */
    private String constructLogMessage(List<MetricDatum> batch)
    {
        StringBuilder sb = new StringBuilder(512);

        sb.append("flush sending ").append(batch.size()).append(" metric");
        if (batch.size() > 1)
            sb.append("s");

        if (batchLoggingLevel == LoggingLevel.NAMES)
        {
            sb.append(": ");
            for (MetricDatum datum : batch)
                sb.append(datum.getMetricName()).append(", ");
            sb.delete(sb.length() - 2, sb.length());
        }

        if (batchLoggingLevel == LoggingLevel.DETAILED)
        {
            sb.append(":");
            for (MetricDatum datum : batch)
            {
                sb.append("\n    ")
                  .append(datum.getMetricName()).append(": ")
                  .append(datum.getValue()).append(" ")
                  .append(datum.getUnit()).append(" @ ")
                  .append(datum.getStorageResolution());
            }
        }

        return sb.toString();
    }
}
