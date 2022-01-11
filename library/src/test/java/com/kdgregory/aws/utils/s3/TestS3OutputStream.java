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

import com.kdgregory.aws.utils.testhelpers.mocks.MockAmazonS3;


// note: this is also a test for MultipartUpload

public class TestS3OutputStream
{
    private MockAmazonS3 mock = new MockAmazonS3();
    private AmazonS3 s3Client = mock.getInstance();

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
        final int numBytes = 1234;
        final byte[] data = randomBytes(numBytes);

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
        assertEquals("content length",              numBytes,       mock.getInvocationArg("putObject", 0, 3, ObjectMetadata.class).getContentLength());
        assertArrayEquals("content",                data,           mock.buffers.get(0));
    }


    @Test
    public void testMultiPartSmallWrites() throws Exception
    {
        final int numBytes = 11 * 1024 * 1024;
        final byte[] data = randomBytes(numBytes);

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            for (int ii = 0 ; ii < 11 * 1024 ; ii++)
            {
                out.write(data, ii * 1024, 1024);
            }
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
        assertEquals("buffer 1 size",               5 * 1024 * 1024,    mock.buffers.get(0).length);
        assertEquals("buffer 2 size",               5 * 1024 * 1024,    mock.buffers.get(1).length);
        assertEquals("buffer 3 size",               1 * 1024 * 1024,    mock.buffers.get(2).length);

        assertEquals("part 1 reported size",        5 * 1024 * 1024,    mock.getInvocationArg("uploadPart", 0, 0, UploadPartRequest.class).getPartSize());
        assertEquals("part 2 reported size",        5 * 1024 * 1024,    mock.getInvocationArg("uploadPart", 1, 0, UploadPartRequest.class).getPartSize());
        assertEquals("part 3 reported size",        1 * 1024 * 1024,    mock.getInvocationArg("uploadPart", 2, 0, UploadPartRequest.class).getPartSize());
    }


    @Test
    public void testMultiPartLargeWrite() throws Exception
    {
        final int numBytes = 11 * 1024 * 1024;
        final byte[] data = randomBytes(numBytes);

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
    public void testMultiPartExplicitBufferSize() throws Exception
    {
        final int partSize = 7 * 1024 * 1024;
        final int numBytes = 11 * 1024 * 1024;
        final byte[] data = randomBytes(numBytes);

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
        final int numBytes = 4 * 1024 * 1024;
        final byte[] data = randomBytes(numBytes);

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
        final int numBytes = 4 * 1024 * 1024;
        final byte[] data = randomBytes(numBytes);

        try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
        {
            out.write(data);
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
        final int numBytes = 1 * 1024 * 1024;
        final byte[] data = randomBytes(numBytes);

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
        assertEquals("updated content length",      numBytes,       mock.getInvocationArg("putObject", 0, 3, ObjectMetadata.class).getContentLength());
    }


    @Test
    public void testExplicitObjectMetadataMultiPart() throws Exception
    {
        final int numBytes = 11 * 1024 * 1024;
        final byte[] data = randomBytes(numBytes);

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
        assertEquals("calls to uploadPart",         3,                  mock.getInvocationCount("uploadPart"));
        assertEquals("completed multipart",         1,                  mock.getInvocationCount("completeMultipartUpload"));

        assertSame("passed provied metadata",       meta,               mock.getInvocationArg("initiateMultipartUpload", 0, 0, InitiateMultipartUploadRequest.class).getObjectMetadata());
    }

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    private static byte[] randomBytes(int length)
    {
        byte[] data = new byte[length];
        (new Random()).nextBytes(data);
        return data;
    }
}
