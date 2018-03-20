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

package com.kdgregory.aws.utils.examples.kinesis;

import java.util.Date;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.amazonaws.util.BinaryUtils;

import com.kdgregory.aws.utils.kinesis.KinesisReader;


/**
 *  Reads records from the stream passed on the command-line.
 */
public class KinesisReaderExample
{
    public static void main(String[] argv)
    throws Exception
    {
        AmazonKinesis client = AmazonKinesisClientBuilder.defaultClient();
        KinesisReader reader = new KinesisReader(client, argv[0], ShardIteratorType.TRIM_HORIZON);

        while (true)
        {
            System.out.println();
            System.out.println();
            System.out.println(new Date());
            System.out.println("Offsets: " + reader.getOffsets());

            for (Record record : reader)
            {
                byte[] data = BinaryUtils.copyAllBytesFrom(record.getData());
                String text = new String(data, "UTF-8").trim();
                System.out.println();
                System.out.println("sequence number: " + record.getSequenceNumber());
                System.out.println("partition key:   " + record.getPartitionKey());
                System.out.println("content:         " + text);
            }

            Thread.sleep(250);
        }
    }
}
