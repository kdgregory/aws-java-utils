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

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;

import com.kdgregory.aws.utils.kinesis.KinesisWriter;

/**
 *  Simple stream writer: writes each line of input from the console to
 *  the stream.
 *  <p>
 *  Invocation:
 *  <pre>
 *    java -cp target/aws-java-utils-examples-*.jar com.kdgregory.aws.utils.examples.kinesis.KinesisWriterExample STREAM_NAME [RECORDS_PER_BATCH]
 *  </pre>
 *  Where:
 *  <ul>
 *  <li> STREAM_NAME is the name of the destination stream
 *  <li> RECORDS_PER_BATCH specifies the number of records that should be batched
 *       together (default is 1)
 *  </ul>
 */
public class KinesisWriterExample
{
    public static void main(String[] argv)
    throws Exception
    {
        String streamName = argv[0];
        int recordsPerBatch = (argv.length == 2) ? Integer.parseInt(argv[1]) : 1;

        AmazonKinesis client = AmazonKinesisClientBuilder.defaultClient();
        KinesisWriter writer = new KinesisWriter(client, streamName);

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        int recordCount = 0;
        String inputLine = "";
        while ((inputLine = in.readLine()) != null)
        {
            writer.addRecord(null, inputLine);
            // sending fixed-size batches is inefficient; in the real world we'd
            // only send when addRecord() returned false
            if (++recordCount % recordsPerBatch == 0)
            {
                writer.send();
                // we would normally insert a sleep after sending, but since this is
                // an interactive program we don't need to worry about
            }
        }

        System.err.println("ensuring that all outstanding records have been written");
        while (writer.getUnsentRecords().size() > 0)
        {
            writer.send();
            Thread.sleep(1000);
        }
    }
}
