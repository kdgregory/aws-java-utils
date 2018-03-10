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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.*;

/**
 *  Reports metrics to CloudWatch using a background thread.
 *  <p>
 *  A single instance of this class would typically be shared throughout an
 *  application, writing all metrics to the same namespace. At construction
 *  time you can provide a map of default dimensions that are applied to all
 *  metrics (for example, to differentiate between production and test).
 *  <p>
 *  Example: create an instance with the "com.example.application" namespace and
 *  "test" default dimension. This constructor creates its own AWS client and
 *  background thread; you could alternatively pass these in.
 *  <pre>
 *      Map<String,String> defaultDimensions = new HashMap<String,String>();
 *      defaultDimensions.put("environment", "test");
 *
 *      MetricReporter reporter = new MetricReporter("com.example.application", defaultDimensions);
 *  </pre>
 *  After construction call {@link #report} to report metrics. You can specify
 *  the units that are applied to the metric (eg, bytes/second) or default to
 *  the "count" unit.
 *  <p>
 *  Example: the following code might be used to monitor web requests by URL.
 *  The first call simply increments the count of requests, while the second
 *  tracks the number of bytes returned. Note that this could beceome very
 *  expensive with a high-volume server: each URL will be a separate metric
 *  with baseline charge, plus you are charged per call.
 *  <pre>
 *      String url = // extract from request
 *      long contentLength = // extract from response
 *
 *      Map<String,String> dimensions = new HashMap<String,String>();
 *      dimensions.put("URL", url);
 *
 *      reporter.report("requestsProcessed", dimensions, 1);
 *      reporter.report("requestsProcessed", dimensions, contentLength, StandardUnit.Bytes);
 *  </pre>
 *  To minimize the number of <code>PutMetricData</code> calls (which are charged
 *  individually), you can configure the reporter to batch requests. Batch size is
 *  driven by two configuration parameters: number of items and delay. After one
 *  call to {@link #report}, subsequent items will be added to the batch until
 *  either the delay has expired or the desired number of items has been added. In
 *  general, you should pick delays in the range of a few seconds, to avoid losing
 *  data in the event of application termination. There is also a 40 kbyte limit to
 *  a request, which should be sufficient to hold dozens of metrics (depending on
 *  how many dimensions they have).
 *  <p>
 *  Exceptions that happen while reporting metrics are logged, and the metrics are
 *  discarded. This is only expected to happen with misconfigured credentials: the
 *  <code>PutMetricData</code> documented exceptions should not occur when using
 *  the SDK to create a request.
 */
public class MetricReporter
{
    private Log logger = LogFactory.getLog(getClass());

    private AmazonCloudWatch    client;
    private String              namespace;
    private Map<String,String>  defaultDimensions;
    private int                 batchSize;
    private long                batchDelay;

    private MetricSender        sender;

    /**
     *  Base constructor, allowing full configuration.
     *
     *  @param  client              CloudWatch client, configured for desired region.
     *  @param  executor            Provides access to background thread for sending metrics.
     *                              While this can be a thread pool, be aware that the writer
     *                              job never stops.
     *  @param  namespace           Used to group all metrics reported by this object. Typically
     *                              one namespace is used across an application.
     *  @param  defaultDimensions   Gefault dimensions that will be added to every request.
     *                              Typically used to hold information such as environmeent
     *                              (dev/qa/prod/...) or source (hostname, instance ID, ...).
     *  @param  batchSize           The maxmum number of metrics that will be batched. This
     *                              will be dependent on the number of dimensions in the metric
     *                              and the size of each dimension.
     *  @param  batchDelay          The number of milliseconds that the sender will wait to
     *                              compile a batch.
     */
    public MetricReporter(
        AmazonCloudWatch    client,
        Executor            executor,
        String              namespace,
        Map<String,String>  defaultDimensions,
        int                 batchSize,
        long                batchDelay)
    {
        this.client = client;
        this.namespace = namespace;
        this.defaultDimensions = defaultDimensions;
        this.batchSize = batchSize;
        this.batchDelay = batchDelay;

        sender = new MetricSender();
        executor.execute(sender);
    }

    /**
     *  Convenience constructor, which sets a maximum batch size of 1
     *  with no delay, but otherwise allows configuration.
     */
    public MetricReporter(
        AmazonCloudWatch    client,
        Executor            executor,
        String              namespace,
        Map<String,String>  defaultDimensions)
    {
        this(client, executor, namespace, defaultDimensions, 1, 0);
    }


    /**
     *  Convenience constructor, which creates a client and single-thread
     *  threadpool. This constructor requires SDK version 1.11.16 or above.
     */
    public MetricReporter(String namespace, Map<String,String> defaultDimensions)
    {
        this(namespace);
        this.defaultDimensions = defaultDimensions;
    }


    /**
     *  Convenience constructor, which creates a client and single-thread
     *  threadpool. This constructor requires SDK version 1.11.16 or above.
     */
    public MetricReporter(String namespace)
    {
        this(AmazonCloudWatchClientBuilder.defaultClient(),
             Executors.newSingleThreadExecutor(),
             namespace,
             Collections.<String,String>emptyMap());
    }

//----------------------------------------------------------------------------
//  Public methods
//----------------------------------------------------------------------------

    /**
     *  Reports a standard-resolution "count" metric with default dimensions
     *  (if any).
     *
     *  @param  name        Unique name for the metric.
     *  @param  value       Value of the metric.
     */
    public void report(String name, double value)
    {
        report(name, value, StandardUnit.Count);
    }


    /**
     *  Reports a standard-resolution metric with default dimensions
     *  (if any).
     *
     *  @param  name        Unique name for the metric.
     *  @param  value       Value of the metric.
     *  @param  units       The units used for metric reporting (eg: kilobytes, kb/second).
     */
    public void report(String name, double value, StandardUnit units)
    {
        report(name, Collections.<String,String>emptyMap(), value, units);
    }


    /**
     *  Reports a standard-resolution "count" metric with default dimensions
     *  (if any) and explicit dimensions.
     *
     *  @param  name        Unique name for the metric.
     *  @param  dimensions  The dimensions that apply to this value.
     *  @param  value       Value of the metric.
     */
    public void report(String name, Map<String,String> dimensions, double value)
    {
        report(name, dimensions, value, StandardUnit.Count);
    }


    /**
     *  Reports a standard-resolution metric with default dimensions
     *  (if any) and explicit dimensions.
     *
     *  @param  name        Unique name for the metric.
     *  @param  dimensions  The dimensions that apply to this value.
     *  @param  value       Value of the metric.
     *  @param  units       The units used for metric reporting (eg: kilobytes, kb/second).
     */
    public void report(String name, Map<String,String> dimensions, double value, StandardUnit units)
    {
        sender.addMetric(createDatum(name, dimensions, value, units));
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Helper method to create the MetricDatum object, using both default and
     *  explicit dimensions. The "hires" reporting methods will change this,
     *  the "standard" methods will leave it alone.
     */
    private MetricDatum createDatum(String name, Map<String,String> explicitDimensions, double value, StandardUnit units)
    {
        List<Dimension> dimensions = new ArrayList<Dimension>();
        for (Map.Entry<String,String> dim : defaultDimensions.entrySet())
        {
            dimensions.add(new Dimension().withName(dim.getKey()).withValue(dim.getValue()));
        }
        for (Map.Entry<String,String> dim : explicitDimensions.entrySet())
        {
            dimensions.add(new Dimension().withName(dim.getKey()).withValue(dim.getValue()));
        }

        return new MetricDatum()
                .withMetricName(name)
                .withDimensions(dimensions)
                .withTimestamp(new Date())
                .withUnit(units)
                .withValue(value);
    }


    /**
     *  This object runs on the background thread, batching and sending requests.
     */
    private class MetricSender implements Runnable
    {
        private LinkedBlockingQueue<MetricDatum> queue = new LinkedBlockingQueue<MetricDatum>();

        public void addMetric(MetricDatum metric)
        {
            queue.add(metric);
        }

        @Override
        public void run()
        {
            while (true)
            {
                List<MetricDatum> metrics = dequeueMetricData();
                if (! metrics.isEmpty())
                {
                    PutMetricDataRequest request = new PutMetricDataRequest()
                                                   .withNamespace(namespace)
                                                   .withMetricData(metrics);
                    try
                    {
                        client.putMetricData(request);
                    }
                    catch (Exception ex)
                    {
                        logger.error("failed to write metrics: " + request, ex);
                    }
                }
            }
        }

        private List<MetricDatum> dequeueMetricData()
        {
            List<MetricDatum> result = new ArrayList<MetricDatum>();
            try
            {
                // we'll wait "forever" for first datum
                result.add(queue.take());

                // then set a window to fill up the batch
                long batchStart = System.currentTimeMillis();
                long batchFinish = batchStart + batchDelay;
                while ((System.currentTimeMillis() < batchFinish) && (result.size() < batchSize))
                {
                    MetricDatum metric = queue.poll(batchFinish - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if (metric != null)
                        result.add(metric);
                }
            }
            catch (InterruptedException ignored)
            {
                // fall through to return
            }

            return result;
        }
    }
}
