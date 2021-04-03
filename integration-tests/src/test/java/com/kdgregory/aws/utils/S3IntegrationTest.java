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

package com.kdgregory.aws.utils;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.kdgcommons.io.IOUtil;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.kdgregory.aws.utils.s3.ObjectListIterable;
import com.kdgregory.aws.utils.s3.S3OutputStream;


public class S3IntegrationTest
{
    private static Logger logger = LoggerFactory.getLogger(S3IntegrationTest.class);

    private static String bucketName = UUID.randomUUID().toString();
    private static AmazonS3 s3Client;

//----------------------------------------------------------------------------
//  Create/destroy the test bucket
//----------------------------------------------------------------------------

    @BeforeClass
    public static void initialize()
    throws Exception
    {
        s3Client = AmazonS3ClientBuilder.defaultClient();
        logger.info("creating bucket: {}", bucketName);
        s3Client.createBucket(bucketName);
    }


    @AfterClass
    public static void cleanup()
    throws Exception
    {
        logger.info("emptying bucket");
        // this is an incidental test for ObjectListIterable
        for (S3ObjectSummary obj : new ObjectListIterable(s3Client, bucketName))
        {
            logger.debug("deleting {}", obj.getKey());
            s3Client.deleteObject(bucketName, obj.getKey());
        }

        logger.info("deletingbucket");
        s3Client.deleteBucket(bucketName);
        s3Client.shutdown();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testS3OutputStreamSmallFile() throws Exception
    {
        String key = UUID.randomUUID().toString();
        byte[] data = "hello, world".getBytes(StandardCharsets.UTF_8);

        logger.info("testS3OutputStreamSmallFile: uploading to {}", key);
        try (OutputStream out = new S3OutputStream(s3Client, bucketName, key))
        {
            out.write(data);
        }
        downloadAndAssert(key, data);
    }


    @Test
    public void testS3OutputStreamLargeFile() throws Exception
    {
        String key = UUID.randomUUID().toString();
        byte[] data = new byte[14 * 1024 * 1024];
        (new Random()).nextBytes(data);

        logger.info("testS3OutputStreamSmallFile: uploading to {}", key);
        try (OutputStream out = new S3OutputStream(s3Client, bucketName, key))
        {
            out.write(data);
        }
        downloadAndAssert(key, data);
    }

//----------------------------------------------------------------------------
//  Support Code
//----------------------------------------------------------------------------

    private void downloadAndAssert(String key, byte[] expected)
    throws Exception
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream(expected.length);
        S3Object obj = s3Client.getObject(bucketName, key);
        try (InputStream in = obj.getObjectContent())
        {
            IOUtil.copy(in, out);
        }

        assertArrayEquals("downloaded file", expected, out.toByteArray());
    }
}
