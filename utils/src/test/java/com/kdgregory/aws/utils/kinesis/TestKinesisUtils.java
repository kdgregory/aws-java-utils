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

package com.kdgregory.aws.utils.kinesis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.*;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;


/**
 *  Mock-object tests of KinesisUtils.
 */
public class TestKinesisUtils
{
    @Test
    public void testDescribeShardsSingleRetrieve() throws Exception
    {
        // nope, this isn't a full description, but we're not testing a full return
        final List<Shard> expected = Arrays.asList(
                                        new Shard().withShardId("0001"),
                                        new Shard().withShardId("0002"));

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                assertEquals("request contains stream name", "example", request.getStreamName());
                return new DescribeStreamResult().withStreamDescription(
                        new StreamDescription().withShards(expected).withHasMoreShards(Boolean.FALSE));
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned expected list", expected, shards);
    }


    @Test
    public void testDescribeShardsMultiRetrieve() throws Exception
    {
        final List<Shard> return1 = Arrays.asList(
                                        new Shard().withShardId("0001"),
                                        new Shard().withShardId("0002"));
        final List<Shard> return2 = Arrays.asList(
                                        new Shard().withShardId("0003"),
                                        new Shard().withShardId("0004"));

        final List<Shard> expected = CollectionUtil.combine(new ArrayList<Shard>(), return1, return2);

        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                assertEquals("request contains stream name", "example", request.getStreamName());
                if ("0002".equals(request.getExclusiveStartShardId()))
                {
                    return new DescribeStreamResult().withStreamDescription(
                            new StreamDescription().withShards(return2).withHasMoreShards(Boolean.FALSE));
                }
                else
                {
                    return new DescribeStreamResult().withStreamDescription(
                            new StreamDescription().withShards(return1).withHasMoreShards(Boolean.TRUE));
                }
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned expected list", expected, shards);
    }


    @Test
    public void testStreamNotAvailable() throws Exception
    {
        AmazonKinesis client = new SelfMock<AmazonKinesis>(AmazonKinesis.class)
        {
            @SuppressWarnings("unused")
            public DescribeStreamResult describeStream(DescribeStreamRequest request)
            {
                throw new ResourceNotFoundException("whatever");
            }
        }.getInstance();

        List<Shard> shards = KinesisUtils.describeShards(client, "example", 1000);
        assertEquals("returned empty", null, shards);
    }
}
