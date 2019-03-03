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

package com.kdgregory.aws.utils.testhelpers.mocks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;

/**
 *  A mock that records its invocations. Override to cause errors.
 */
public class MockAmazonCloudWatch
extends SelfMock<AmazonCloudWatch>
{
    public volatile int putMetricDataInvocationCount;

    public List<PutMetricDataRequest> allMetricDataRequests = Collections.synchronizedList(new ArrayList<PutMetricDataRequest>());
    public volatile PutMetricDataRequest lastPutMetricDataRequest;
    public volatile Thread executedOn;

    public MockAmazonCloudWatch()
    {
        super(AmazonCloudWatch.class);
    }


    public PutMetricDataResult putMetricData(PutMetricDataRequest request)
    {
        putMetricDataInvocationCount++;
        allMetricDataRequests.add(request);
        lastPutMetricDataRequest = request;
        executedOn = Thread.currentThread();
        return new PutMetricDataResult();
    }
}