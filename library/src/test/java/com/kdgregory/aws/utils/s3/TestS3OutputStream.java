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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.junit.Test;
import static org.junit.Assert.*;

import net.sf.kdgcommons.io.IOUtil;
import net.sf.kdgcommons.lang.ClassUtil;
import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;


public class TestS3OutputStream
{
    private S3Mock mock = new S3Mock();
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
//  Support Code
//----------------------------------------------------------------------------

    private static byte[] randomBytes(int length)
    {
        byte[] data = new byte[length];
        (new Random()).nextBytes(data);
        return data;
    }


    @SuppressWarnings("unused")
    private static class S3Mock
    extends SelfMock<AmazonS3>
    {
        public S3Mock()
        {
            super(AmazonS3.class);
        }

        // internals -- these are used to assert locally

        public String multipartBucket;
        public String multipartKey;
        private String uploadToken;
        public List<PartETag> partTags = new ArrayList<>();

        // recorded information separate from that recorded by SelfMock -- used for caller assertions

        public List<byte[]> buffers = new ArrayList<>();

        // helper functions

        private void extractBuffer(InputStream in)
        {
            try
            {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                IOUtil.copy(in, bos);
                buffers.add(bos.toByteArray());
            }
            catch (IOException ex)
            {
                throw new RuntimeException("internal exception in mock", ex);
            }
        }

        public byte[] recombineBuffers()
        throws Exception
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (byte[] buffer : buffers)
            {
                bos.write(buffer);
            }
            return bos.toByteArray();
        }

        // mock handlers follow

        public PutObjectResult putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata)
        {
            extractBuffer(input);
            // we don't look at the result, so no reason to return it
            return new PutObjectResult();
        }

        public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
        {
            multipartBucket = request.getBucketName();
            multipartKey = request.getKey();
            uploadToken = UUID.randomUUID().toString();  // just need something

            InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
            result.setUploadId(uploadToken);
            return result;
        }

        public UploadPartResult uploadPart(UploadPartRequest request)
        {
            assertEquals("uploadPart specified bucket",         multipartBucket,    request.getBucketName());
            assertEquals("uploadPart specified key",            multipartKey,       request.getKey());
            assertEquals("uploadPart specified upload token",   uploadToken,        request.getUploadId());

            extractBuffer(request.getInputStream());
            PartETag partTag = new PartETag(request.getPartNumber(), UUID.randomUUID().toString());
            partTags.add(partTag);

            UploadPartResult result = new UploadPartResult();
            result.setPartNumber(request.getPartNumber());
            result.setETag(partTag.getETag());
            return result;
        }

        public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
        {
            assertEquals("completeMultipartUpload specified bucket",        multipartBucket,    request.getBucketName());
            assertEquals("completeMultipartUpload specified key",           multipartKey,       request.getKey());
            assertEquals("completeMultipartUpload specified upload token",  uploadToken,        request.getUploadId());

            // PartETag is does not support value-based equals, so we need to compare explicitly
            assertEquals("completeMultipartUpload number of part tags",     partTags.size(),    request.getPartETags().size());
            for (int ii = 0 ; ii < partTags.size() ; ii++)
            {
                assertEquals("completeMultipartUpload part tag " + ii + " partNumber",  partTags.get(ii).getPartNumber(),   request.getPartETags().get(ii).getPartNumber());
                assertEquals("completeMultipartUpload part tag " + ii + " eTag",        partTags.get(ii).getETag(),         request.getPartETags().get(ii).getETag());
            }

            return new CompleteMultipartUploadResult();
        }

        public void abortMultipartUpload(AbortMultipartUploadRequest request)
        {
            assertEquals("abortMultipartUpload specified bucket",        multipartBucket,    request.getBucketName());
            assertEquals("abortMultipartUpload specified key",           multipartKey,       request.getKey());
            assertEquals("abortMultipartUpload specified upload token",  uploadToken,        request.getUploadId());
        }
    }
}
