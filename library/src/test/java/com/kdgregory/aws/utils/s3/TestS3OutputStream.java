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

package com.kdgregory.aws.utils.s3;

import java.io.ByteArrayOutputStream;
import java.util.Random;

import org.junit.Test;
import static org.junit.Assert.*;

import net.sf.kdgcommons.lang.ClassUtil;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.UploadPartRequest;

import com.kdgregory.aws.utils.testhelpers.TestableExecutorService;
import com.kdgregory.aws.utils.testhelpers.mocks.MockAmazonS3;


// note: this is also a test for MultipartUpload

public class TestS3OutputStream
{
    private MockAmazonS3 mock = new MockAmazonS3();
    private AmazonS3 s3Client = mock.getInstance();

    private Random rnd = new Random();

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testEmptyFile() throws Exception
    {
        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            // nothing here
        }

        assertEquals("did not start multipart",     0,              mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("did not upload part",         0,              mock.getInvocationCount("uploadPart"));
        assertEquals("did not complete multipart",  0,              mock.getInvocationCount("completeMultipartUpload"));
        assertEquals("did not abort multipart",     0,              mock.getInvocationCount("abortMultipartUpload"));

        assertEquals("called PutObject",            1,              mock.getInvocationCount("putObject"));
        assertEquals("bucket",                      "argle",        mock.getInvocationArg("putObject", 0, 0, String.class));
        assertEquals("key",                         "bargle",       mock.getInvocationArg("putObject", 0, 1, String.class));
        assertEquals("content length",              0L,             mock.getInvocationArg("putObject", 0, 3, ObjectMetadata.class).getContentLength());
        assertArrayEquals("content",                new byte[0],    mock.buffers.get(0));
    }


    @Test
    public void testSmallFile() throws Exception
    {
        byte[] data = new byte[1234];
        rnd.nextBytes(data);

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            out.write(data);
        }

        assertEquals("did not start multipart",     0,              mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("did not upload part",         0,              mock.getInvocationCount("uploadPart"));
        assertEquals("did not complete multipart",  0,              mock.getInvocationCount("completeMultipartUpload"));
        assertEquals("did not abort multipart",     0,              mock.getInvocationCount("abortMultipartUpload"));

        assertEquals("called PutObject",            1,              mock.getInvocationCount("putObject"));
        assertEquals("bucket",                      "argle",        mock.getInvocationArg("putObject", 0, 0, String.class));
        assertEquals("key",                         "bargle",       mock.getInvocationArg("putObject", 0, 1, String.class));
        assertEquals("content length",              data.length,    mock.getInvocationArg("putObject", 0, 3, ObjectMetadata.class).getContentLength());
        assertArrayEquals("content",                data,           mock.buffers.get(0));
    }


    @Test
    public void testMultiPartSmallWrites() throws Exception
    {
        // the individual writes are intended to misalign with the buffer size
        byte[] data = new byte[11 * 1024 * 1027];
        rnd.nextBytes(data);

        final int expectedBaseBufferSize = 5 * 1024 * 1024;
        final int expectedLastBufferSize = data.length - 2 * expectedBaseBufferSize;

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            for (int ii = 0 ; ii < 11 * 1024 ; ii++)
            {
                out.write(data, ii * 1027, 1027);
            }
        }

        assertEquals("did not call PutObject",      0,                          mock.getInvocationCount("putObject"));
        assertEquals("did not abort multipart",     0,                          mock.getInvocationCount("abortMultipartUpload"));

        assertEquals("started multipart",           1,                          mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("calls to uploadPart",         3,                          mock.getInvocationCount("uploadPart"));
        assertEquals("completed multipart",         1,                          mock.getInvocationCount("completeMultipartUpload"));

        assertEquals("bucket",                      "argle",                    mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getBucketName());
        assertEquals("key",                         "bargle",                   mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getKey());
        assertArrayEquals("data",                   data,                       mock.recombineBuffers());

        assertEquals("number of buffers",           3,                          mock.buffers.size());
        assertEquals("buffer 1 size",               expectedBaseBufferSize,     mock.buffers.get(0).length);
        assertEquals("buffer 2 size",               expectedBaseBufferSize,     mock.buffers.get(1).length);
        assertEquals("buffer 3 size",               expectedLastBufferSize,     mock.buffers.get(2).length);

        assertEquals("part 1 reported size",        expectedBaseBufferSize,     mock.getInvocationArg("uploadPart", 0, 0, UploadPartRequest.class).getPartSize());
        assertEquals("part 2 reported size",        expectedBaseBufferSize,     mock.getInvocationArg("uploadPart", 1, 0, UploadPartRequest.class).getPartSize());
        assertEquals("part 3 reported size",        expectedLastBufferSize,     mock.getInvocationArg("uploadPart", 2, 0, UploadPartRequest.class).getPartSize());
    }


    @Test
    public void testMultiPartLargeWrite() throws Exception
    {
        byte[] data = new byte[11 * 1024 * 1024];
        rnd.nextBytes(data);

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            out.write(data);
        }

        assertEquals("did not call PutObject",      0,                  mock.getInvocationCount("putObject"));
        assertEquals("did not abort multipart",     0,                  mock.getInvocationCount("abortMultipartUpload"));

        assertEquals("started multipart",           1,                  mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("calls to uploadPart",         3,                  mock.getInvocationCount("uploadPart"));
        assertEquals("completed multipart",         1,                  mock.getInvocationCount("completeMultipartUpload"));

        assertEquals("bucket",                      "argle",            mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getBucketName());
        assertEquals("key",                         "bargle",           mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getKey());
        assertArrayEquals("data",                   data,               mock.recombineBuffers());

        assertEquals("number of buffers",           3,                  mock.buffers.size());
        assertEquals("buffer 0 size",               5 * 1024 * 1024,    mock.buffers.get(0).length);
        assertEquals("buffer 1 size",               5 * 1024 * 1024,    mock.buffers.get(1).length);
        assertEquals("buffer 2 size",               1 * 1024 * 1024,    mock.buffers.get(2).length);

        assertEquals("part 1 reported size",        5 * 1024 * 1024,    mock.getInvocationArg("uploadPart", 0, 0, UploadPartRequest.class).getPartSize());
        assertEquals("part 2 reported size",        5 * 1024 * 1024,    mock.getInvocationArg("uploadPart", 1, 0, UploadPartRequest.class).getPartSize());
        assertEquals("part 3 reported size",        1 * 1024 * 1024,    mock.getInvocationArg("uploadPart", 2, 0, UploadPartRequest.class).getPartSize());
    }


    @Test
    public void testMultiPartLargeWriteExactBufferMultiple() throws Exception
    {
        byte[] data = new byte[10 * 1024 * 1024];
        rnd.nextBytes(data);

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            out.write(data);
        }

        assertEquals("did not call PutObject",      0,                  mock.getInvocationCount("putObject"));
        assertEquals("did not abort multipart",     0,                  mock.getInvocationCount("abortMultipartUpload"));

        assertEquals("started multipart",           1,                  mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("calls to uploadPart",         2,                  mock.getInvocationCount("uploadPart"));
        assertEquals("completed multipart",         1,                  mock.getInvocationCount("completeMultipartUpload"));

        assertEquals("bucket",                      "argle",            mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getBucketName());
        assertEquals("key",                         "bargle",           mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getKey());
        assertArrayEquals("data",                   data,               mock.recombineBuffers());

        assertEquals("number of buffers",           2,                  mock.buffers.size());
        assertEquals("buffer 0 size",               5 * 1024 * 1024,    mock.buffers.get(0).length);
        assertEquals("buffer 1 size",               5 * 1024 * 1024,    mock.buffers.get(1).length);

        assertEquals("part 1 reported size",        5 * 1024 * 1024,    mock.getInvocationArg("uploadPart", 0, 0, UploadPartRequest.class).getPartSize());
        assertEquals("part 2 reported size",        5 * 1024 * 1024,    mock.getInvocationArg("uploadPart", 1, 0, UploadPartRequest.class).getPartSize());
    }


    @Test
    public void testMultiPartExplicitBufferSize() throws Exception
    {
        final int partSize = 7 * 1024 * 1024;
        byte[] data = new byte[11 * 1024 * 1024];
        rnd.nextBytes(data);

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle", new ObjectMetadata(), partSize))
        {
            out.write(data);
        }

        assertEquals("did not call PutObject",      0,                  mock.getInvocationCount("putObject"));
        assertEquals("did not abort multipart",     0,                  mock.getInvocationCount("abortMultipartUpload"));

        assertEquals("started multipart",           1,                  mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("calls to uploadPart",         2,                  mock.getInvocationCount("uploadPart"));
        assertEquals("completed multipart",         1,                  mock.getInvocationCount("completeMultipartUpload"));

        assertEquals("bucket",                      "argle",            mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getBucketName());
        assertEquals("key",                         "bargle",           mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getKey());
        assertArrayEquals("data",                   data,               mock.recombineBuffers());

        assertEquals("number of buffers",           2,                  mock.buffers.size());
        assertEquals("buffer 0 size",               7 * 1024 * 1024,    mock.buffers.get(0).length);
        assertEquals("buffer 1 size",               4 * 1024 * 1024,    mock.buffers.get(1).length);
    }


    @Test
    public void testAbortBeforeWrite() throws Exception
    {
        byte[] data = new byte[4 * 1024 * 1024];
        rnd.nextBytes(data);

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            out.write(data);
            out.abort();

            assertNull("buffer nullified after abort", ClassUtil.getFieldValue(out, "buffer", ByteArrayOutputStream.class));

            // verify that write is silently ignored after abort -- these shouldn't throw
            out.write((byte)1);
            out.write(data);
            out.write(data, 0, 2);
        }

        assertEquals("did not call PutObject",      0,              mock.getInvocationCount("putObject"));
        assertEquals("did not start multipart",     0,              mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("did not call uploadPart",     0,              mock.getInvocationCount("uploadPart"));
        assertEquals("did not complete multipart",  0,              mock.getInvocationCount("completeMultipartUpload"));

        assertEquals("did not call abort",          0,              mock.getInvocationCount("abortMultipartUpload"));
    }


    @Test
    public void testAbortAfterWrite() throws Exception
    {
        // 6 MB forces us to do two parts
        byte[] data = new byte[6 * 1024 * 1024];
        rnd.nextBytes(data);

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            out.write(data);
            out.abort();
        }

        assertEquals("did not call PutObject",      0,              mock.getInvocationCount("putObject"));
        assertEquals("started multipart",           1,              mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("called uploadPart",           1,              mock.getInvocationCount("uploadPart"));
        assertEquals("did not complete multipart",  0,              mock.getInvocationCount("completeMultipartUpload"));

        assertEquals("called abort",                1,              mock.getInvocationCount("abortMultipartUpload"));
    }


    @Test
    public void testExplicitObjectMetadataSmallFile() throws Exception
    {
        byte[] data = new byte[1 * 1024 * 1024];
        rnd.nextBytes(data);

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType("test/test");
        meta.addUserMetadata("argle", "bargle");
        meta.setContentLength(1234);    // this gets overwritten

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle", meta))
        {
            out.write(data);
        }

        assertEquals("did not start multipart",     0,              mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("did not upload part",         0,              mock.getInvocationCount("uploadPart"));
        assertEquals("did not complete multipart",  0,              mock.getInvocationCount("completeMultipartUpload"));
        assertEquals("did not abort multipart",     0,              mock.getInvocationCount("abortMultipartUpload"));

        assertEquals("called PutObject",            1,              mock.getInvocationCount("putObject"));
        assertSame("passed provided metadata",      meta,           mock.getInvocationArg("putObject", 0, 3, ObjectMetadata.class));
        assertEquals("updated content length",      data.length,    mock.getInvocationArg("putObject", 0, 3, ObjectMetadata.class).getContentLength());
    }


    @Test
    public void testExplicitObjectMetadataMultiPart() throws Exception
    {
        byte[] data = new byte[30 * 1024 * 1024];
        rnd.nextBytes(data);

        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentType("test/test");
        meta.addUserMetadata("argle", "bargle");

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle", meta))
        {
            out.write(data);
        }

        assertEquals("did not call PutObject",      0,                  mock.getInvocationCount("putObject"));
        assertEquals("did not abort multipart",     0,                  mock.getInvocationCount("abortMultipartUpload"));

        assertEquals("started multipart",           1,                  mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("calls to uploadPart",         6,                  mock.getInvocationCount("uploadPart"));
        assertEquals("completed multipart",         1,                  mock.getInvocationCount("completeMultipartUpload"));

        assertSame("passed provied metadata",       meta,               mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getObjectMetadata());
    }


    @Test
    @SuppressWarnings("resource")
    public void testConcurrentUploads() throws Exception
    {
        byte[] data = new byte[30 * 1024 * 1024];
        rnd.nextBytes(data);

        TestableExecutorService threadpool = new TestableExecutorService();

        S3OutputStream out = new S3OutputStream(s3Client, threadpool, "argle", "bargle", new ObjectMetadata(), 5 * 1024 * 1024);
        out.write(data);

        assertEquals("number of futures created",                   6, threadpool.getSubmitCount());
        assertEquals("number of futures completed before close()",  0, threadpool.getCompleteCount());

        out.close();

        assertEquals("number of futures completed after close()",   6, threadpool.getCompleteCount());

        assertEquals("started multipart",           1,                  mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("calls to uploadPart",         6,                  mock.getInvocationCount("uploadPart"));
        assertEquals("completed multipart",         1,                  mock.getInvocationCount("completeMultipartUpload"));
    }


    @Test
    @SuppressWarnings("resource")
    public void testConcurrentUploadsLimitOutstanding() throws Exception
    {
        byte[] data = new byte[30 * 1024 * 1024];
        rnd.nextBytes(data);

        TestableExecutorService threadpool = new TestableExecutorService();

        S3OutputStream out = new S3OutputStream(s3Client, threadpool, 4, "argle", "bargle", new ObjectMetadata(), 5 * 1024 * 1024);
        out.write(data);

        assertEquals("number of futures created",                   6, threadpool.getSubmitCount());
        assertEquals("number of futures completed before close()",  2, threadpool.getCompleteCount());

        out.close();

        assertEquals("number of futures completed after close()",   6, threadpool.getCompleteCount());

        assertEquals("started multipart",           1,                  mock.getInvocationCount("initiateMultipartUpload"));
        assertEquals("calls to uploadPart",         6,                  mock.getInvocationCount("uploadPart"));
        assertEquals("completed multipart",         1,                  mock.getInvocationCount("completeMultipartUpload"));
    }
}
