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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.kdgcommons.codec.HexCodec;
import net.sf.kdgcommons.io.IOUtil;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import com.kdgregory.aws.utils.s3.MultipartUpload;
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

        logger.info("testS3OutputStreamLargeFile: uploading to {}", key);
        try (OutputStream out = new S3OutputStream(s3Client, bucketName, key))
        {
            out.write(data);
        }
        downloadAndAssert(key, data);
    }


    @Test
    public void testMultipartUploadInline() throws Exception
    {
        int numChunks = 5;
        String key = UUID.randomUUID().toString();
        Random rnd = new Random();
        MessageDigest digester = MessageDigest.getInstance("MD5");
        byte[] chunk = new byte[1024 * 1024 * 5];

        logger.info("testMultipartUploadInline: uploading to {}", key);

        MultipartUpload upload = new MultipartUpload(s3Client);

        // note: let any exceptions propagate; we destroy bucket, don't need to abort
        upload.begin(bucketName, key);
        for (int ii = 1 ; ii <= numChunks ; ii++)
        {
            rnd.nextBytes(chunk);
            digester.update(chunk);
            upload.uploadPart(chunk, ii == numChunks);
        }

        assertEquals("number of outstanding chunks", 0, upload.outstandingTaskCount());
        upload.complete();

        assertObjectDigest(key, digester.digest());
    }


    @Test
    public void testMultipartUploadConcurrent() throws Exception
    {
        int numChunks = 5;
        String key = UUID.randomUUID().toString();
        Random rnd = new Random();
        MessageDigest digester = MessageDigest.getInstance("MD5");
        byte[] chunk = new byte[1024 * 1024 * 5];

        logger.info("testMultipartUploadConcurrent: uploading to {}", key);

        ExecutorService threadpool = Executors.newFixedThreadPool(4);
        MultipartUpload upload = new MultipartUpload(s3Client, threadpool);

        // note: let any exceptions propagate; we destroy bucket, don't need to abort
        upload.begin(bucketName, key);
        for (int ii = 1 ; ii <= numChunks ; ii++)
        {
            rnd.nextBytes(chunk);
            try (FileOutputStream fos = new FileOutputStream("/tmp/" + key + "." + ii))
            {
                fos.write(chunk);
            }
            digester.update(chunk);
            upload.uploadPart(chunk, ii == numChunks);
        }

        // these should be submitted faster than they can be written, we don't know exact numbers
        assertTrue("number of outstanding chunks before complete()", upload.outstandingTaskCount() > 0);

        upload.complete();
        assertEquals("number of outstanding chunks after complete()", 0, upload.outstandingTaskCount());

        assertObjectDigest(key, digester.digest());

        threadpool.shutdown();
    }

//----------------------------------------------------------------------------
//  Support Code
//----------------------------------------------------------------------------

    private void assertObjectDigest(String key, byte[] expected)
    throws Exception
    {
        MessageDigest digester = MessageDigest.getInstance("MD5");
        S3Object obj = s3Client.getObject(bucketName, key);
        try (InputStream in = obj.getObjectContent())
        {
            int r;
            byte[] buf = new byte[8192];
            while ((r = in.read(buf)) > 0)
            {
                digester.update(buf, 0, r);
            }
        }

        byte[] actual = digester.digest();
        assertEquals("object MD5 matches upload digest",
                     new HexCodec().toString(expected).toLowerCase(),
                     new HexCodec().toString(actual).toLowerCase());
    }


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
