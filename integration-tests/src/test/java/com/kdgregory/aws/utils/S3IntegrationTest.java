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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.kdgcommons.codec.HexCodec;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
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

    // a couple of common objects, recreated for each test
    private Random rnd;
    private String key;
    private MessageDigest digester;

//----------------------------------------------------------------------------
//  Setup for all tests in this file
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
//  Per-test setup
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    throws Exception
    {
        rnd = new Random();
        key = UUID.randomUUID().toString();
        digester = MessageDigest.getInstance("MD5");
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testMultipartUploadInline() throws Exception
    {
        logger.info("testMultipartUploadInline: uploading to {}", key);

        int numChunks = 3;
        byte[] chunk = new byte[1024 * 1024 * 5];

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
    public void testMultipartUploadInlinePartialBuffer() throws Exception
    {
        logger.info("testMultipartUploadInlinePartialBuffer: uploading to {}", key);

        int numChunks = 3;
        byte[] chunk = new byte[1024 * 1024 * 6];
        int off = 1024;
        int len = 1024 * 1024 * 5;

        MultipartUpload upload = new MultipartUpload(s3Client);

        // note: let any exceptions propagate; we destroy bucket, don't need to abort
        upload.begin(bucketName, key);
        for (int ii = 1 ; ii <= numChunks ; ii++)
        {
            rnd.nextBytes(chunk);
            digester.update(chunk, off, len);
            upload.uploadPart(chunk, off, len, ii == numChunks);
        }

        assertEquals("number of outstanding chunks", 0, upload.outstandingTaskCount());
        upload.complete();

        assertObjectDigest(key, digester.digest());
    }


    @Test
    public void testMultipartUploadConcurrent() throws Exception
    {
        logger.info("testMultipartUploadConcurrent: uploading to {}", key);

        int numChunks = 5;
        byte[] chunk = new byte[1024 * 1024 * 5];

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


    @Test
    public void testMultipartUploadConcurrentPartialBuffer() throws Exception
    {
        logger.info("testMultipartUploadConcurrentPartialBuffer: uploading to {}", key);

        int numChunks = 5;
        byte[] chunk = new byte[1024 * 1024 * 6];
        int off = 1024;
        int len = 1024 * 1024 * 5;

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
            digester.update(chunk, off, len);
            upload.uploadPart(chunk, off, len, ii == numChunks);
        }

        // these should be submitted faster than they can be written, we don't know exact numbers
        assertTrue("number of outstanding chunks before complete()", upload.outstandingTaskCount() > 0);

        upload.complete();
        assertEquals("number of outstanding chunks after complete()", 0, upload.outstandingTaskCount());

        assertObjectDigest(key, digester.digest());

        threadpool.shutdown();
    }


    @Test
    public void testMultipartUploadWithMetadata() throws Exception
    {
        logger.info("testMultipartUploadWithMetadata: uploading to {}", key);

        int numChunks = 2;
        byte[] chunk = new byte[1024 * 1024 * 5];

        MultipartUpload upload = new MultipartUpload(s3Client);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("application/x-test");

        upload.begin(bucketName, key, metadata);
        for (int ii = 1 ; ii <= numChunks ; ii++)
        {
            rnd.nextBytes(chunk);
            digester.update(chunk);
            upload.uploadPart(chunk, ii == numChunks);
        }

        assertEquals("number of outstanding chunks", 0, upload.outstandingTaskCount());
        upload.complete();

        assertObjectDigest(key, digester.digest());

        ObjectMetadata retrieved = s3Client.getObjectMetadata(bucketName, key);
        assertEquals("metadata", metadata.getContentType(), retrieved.getContentType());
    }


    @Test
    public void testS3OutputStreamInlineSmallFile() throws Exception
    {
        byte[] data = "hello, world".getBytes(StandardCharsets.UTF_8);

        logger.info("testS3OutputStreamSmallFile: uploading to {}", key);
        try (OutputStream out = new S3OutputStream(s3Client, bucketName, key))
        {
            out.write(data);
        }

        assertObjectDigest(key, digester.digest(data));
    }


    @Test
    public void testS3OutputStreamInlineLargeFile() throws Exception
    {
        logger.info("testS3OutputStreamLargeFile: uploading to {}", key);

        // the size of this buffer means that we won't neatly fit into the upload size
        byte[] data = new byte[1024 * 1024 + 17];

        try (OutputStream out = new S3OutputStream(s3Client, bucketName, key))
        {
            for (int ii = 0 ; ii < 14 ; ii++)
            {
                rnd.nextBytes(data);
                digester.update(data);
                out.write(data);
            }
        }

        assertObjectDigest(key, digester.digest());
    }


    @Test
    public void testS3OutputStreamInlineLargeFileConcurrent() throws Exception
    {
        logger.info("testS3OutputStreamInlineLargeFileConcurrent: uploading to {}", key);

        // the size of this buffer means that we won't neatly fit into the upload size
        byte[] data = new byte[1024 * 1024 + 17];

        ExecutorService threadpool = Executors.newFixedThreadPool(4);

        // when this completes, the file should be fully uploaded
        try (OutputStream out = new S3OutputStream(s3Client, threadpool, bucketName, key, new ObjectMetadata(), 5 * 1024 * 1024))
        {
            for (int ii = 0 ; ii < 14 ; ii++)
            {
                rnd.nextBytes(data);
                digester.update(data);
                out.write(data);
            }
        }

        threadpool.shutdown();

        assertObjectDigest(key, digester.digest());
    }

//----------------------------------------------------------------------------
//  Support Code
//----------------------------------------------------------------------------

    private void assertObjectDigest(String objectKey, byte[] expectedDigest)
    throws Exception
    {
        MessageDigest downloadDigester = MessageDigest.getInstance("MD5");
        S3Object obj = s3Client.getObject(bucketName, objectKey);
        try (InputStream in = obj.getObjectContent())
        {
            int r;
            byte[] buf = new byte[8192];
            while ((r = in.read(buf)) > 0)
            {
                downloadDigester.update(buf, 0, r);
            }
        }

        byte[] actualDigest = downloadDigester.digest();
        assertEquals("object MD5 matches upload digest",
                     new HexCodec().toString(expectedDigest).toLowerCase(),
                     new HexCodec().toString(actualDigest).toLowerCase());
    }
}
