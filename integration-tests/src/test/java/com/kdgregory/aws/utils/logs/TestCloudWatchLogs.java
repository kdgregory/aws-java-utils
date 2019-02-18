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

package com.kdgregory.aws.utils.logs;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;


/**
 *  Combined test for all CloudWatch Logs functionality.
 */
public class TestCloudWatchLogs
{
    // a single instance of the client is shared between all tests
    private static AWSLogs client;

//----------------------------------------------------------------------------
//  Pre/post operations
//----------------------------------------------------------------------------

    @BeforeClass
    public static void beforeClass()
    {
        client = AWSLogsClientBuilder.defaultClient();
    }


    @AfterClass
    public static void afterClass()
    {
        client.shutdown();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testCreationAndDeletion() throws Exception
    {
        String namePrefix = "TestLogsUtil-testBasicOperation";
        String logGroupName = namePrefix + "-logGroup-" + UUID.randomUUID();
        String logStreamName = namePrefix + "-logStream-" + UUID.randomUUID();

        // this will also exercise createLogGroup()
        assertNotNull("creation succeeded",                     CloudWatchLogsUtil.createLogStream(client, logGroupName, logStreamName, 30000));

        assertNotNull("post-create describeLogGroup()",         CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNotNull("post-create describeLogStream()",        CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));

        assertTrue("stream deletion succeeded",                  CloudWatchLogsUtil.deleteLogStream(client, logGroupName, logStreamName, 30000));

        assertNotNull("post-delete-stream describeLogGroup()",  CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNull("post-delete-stream describeLogStream()",    CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));

        assertTrue("group deletion succeeded",                  CloudWatchLogsUtil.deleteLogGroup(client, logGroupName, 30000));

        assertNull("post-delete-group describeLogGroup()",      CloudWatchLogsUtil.describeLogGroup(client, logGroupName));
        assertNull("post-delete-group describeLogStream()",     CloudWatchLogsUtil.describeLogStream(client, logGroupName, logStreamName));
    }
    
    
    @Test
    public void testWriterAndReader() throws Exception 
    {
        String namePrefix = "TestLogsUtil-testWriterAndReader";
        String logGroupName = namePrefix + "-logGroup-" + UUID.randomUUID();
        String logStreamName = namePrefix + "-logStream-" + UUID.randomUUID();
        
        long now = System.currentTimeMillis();
        
        CloudWatchWriter writer = new CloudWatchWriter(client, logGroupName, logStreamName);
        
        for (int ii = 0 ; ii < 1000 ; ii++)
        {
            writer.add(now - 15 * ii, String.format("message %d", ii));
        }
        writer.flush();
        
    }
    

}
