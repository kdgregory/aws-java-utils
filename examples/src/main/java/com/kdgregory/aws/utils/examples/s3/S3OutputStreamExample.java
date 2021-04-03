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

package com.kdgregory.aws.utils.examples.s3;

import java.net.ServerSocket;
import java.net.Socket;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.kdgcommons.io.IOUtil;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import com.kdgregory.aws.utils.s3.S3OutputStream;


/**
 *  Listens for network connections and uploads received data to S3 using a key based
 *  on the source IP address and current time.
 *  <p>
 *  Invocation: <code>S3OutputStreamExample PORT BUCKET</code>
 *  <p>
 *
 */
public class S3OutputStreamExample
{
    private final static Logger logger = LoggerFactory.getLogger(S3OutputStreamExample.class);

    private final static int PART_SIZE = 10 * 1024 * 1024;


    public static void main(String[] argv)
    throws Exception
    {
        int port = Integer.parseInt(argv[0]);
        String bucket = argv[1];

        AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

        try (ServerSocket ss = new ServerSocket(port))
        {
            ss.setReuseAddress(false);
            logger.info("listening on {}", ss.getLocalSocketAddress());

            while (true)
            {
                Socket s = ss.accept();
                (new Thread(new Uploader(s3Client, s, bucket))).run();
            }
        }
    }


    private static class Uploader
    implements Runnable
    {
        private AmazonS3 s3Client;
        private Socket s;
        private String bucket;

        public Uploader(AmazonS3 s3Client, Socket s, String bucket)
        {
            this.s3Client = s3Client;
            this.s = s;
            this.bucket = bucket;
        }

        @Override
        public void run()
        {
            try
            {
                // this should create a unique name for each incoming file
                String key = s.getInetAddress().toString().replace("/", "")
                           + "/"
                           + Instant.now().toString().replace('-', '_').replace('T', '_').replace(':', '_').replaceAll("\\..*$", "")
                           + "/"
                           + s.getLocalPort();

                logger.info("received connection, uploading to s3://{}/{}", bucket, key);
                long count = -1;
                try (S3OutputStream out = new S3OutputStream(s3Client, bucket, key, new ObjectMetadata(), PART_SIZE))
                {
                    count = IOUtil.copy(s.getInputStream(), out);
                }
                logger.info("uploaded {} bytes", count);
            }
            catch (Exception ex)
            {
                logger.error("upload failed", ex);
            }
            finally
            {
                IOUtil.closeQuietly(s);
            }
        }
    }
}
