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

import java.util.Iterator;
import java.util.Map;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;


/**
 *  An instantiable class that represents a Kinesis stream as an iterable,
 *  and provides support for saving the current shard offsets.
 */
public class KinesisReader
implements Iterable<Record>
{
    public KinesisReader(AmazonKinesis client, String streamName, Map<String,String> offsets, ShardIteratorType defaultIteratorType)
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }

    
    @Override
    public Iterator<Record> iterator()
    {
        throw new UnsupportedOperationException("FIXME - implement");
    }
}
