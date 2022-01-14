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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;


/**
 *  A stream that transparently writes to S3, so that the program doesn't have to
 *  explicitly buffer output. Will transition from using PutObject to a multi-part
 *  upload, based on the amount of data written. Allows use of a threadpool to do
 *  uploads, so that the program can overlap data generation and physical writes. 
 *  <p>
 *  Use as with any other output stream, with one caveat: you should wrap all code
 *  in a try/catch, to abort the stream on any exception. For example:
 *  <p>
 *  <pre>
 *      InputStream in = // something
 *      try (S3OutputStream out = new S3OutputStream(s3Client, "argle", "bargle"))
 *      {
 *          try
 *          {
 *              IOUtil.copy(in, out);
 *          }
 *          catch (Exception ex)
 *          {
 *              out.abort();
 *              // rethrow or log exception
 *          }
 *      }
 *  </pre>
 *  <p>
 *  Warning: if you don't call {@link #close} or {@link #abort}, you will be left
 *  with an incomplete multi-part upload. This will incur monthly charges for S3
 *  storage. To avoid such charges, you configure your bucket with a life-cycle
 *  rule that deletes incomplete uploads.
 *  <p>
 *  Internally, this stream stores all writes in a buffer until it's accumulated
 *  enough to upload a part (by default, 5 MB). At that point it uploads the part
 *  (inline or on a threadpool) and resets the buffer. On close, it writes all 
 *  outstanding data as a final part, and waits for all parts to finish uploading.
 *  When <code>close()</code> returns, the file will be in S3.
 *  <p>
 *  Implementation note: the actual upload starts when the buffer first fills. An
 *  improperly-configured client will not be discovered until then.
 */
public class S3OutputStream
extends OutputStream
{
    private Log logger = LogFactory.getLog(getClass());

    // these are set by constructor
    private AmazonS3 client;
    private ExecutorService threadpool;
    private int maxOutstanding;
    private String bucket;
    private String key;
    private int partSize;
    private ObjectMetadata metadata;

    // this is assigned once the upload has started
    private MultipartUpload upload;

    // this is assigned at construction, nulled at completion or if aboarted
    private ExposedByteArrayOutputStream buffer;


    /**
     *  Constructs an instance that uploads chunks inline, with the default part
     *  size (5 MB) and default object metadata.
     *  <p>
     *  Uploads using this constructor are limited to 50GB; if you need to
     *  upload more data, you must specify a larger part size.
     *
     *  @param  client      The S3 client that will be used for uploading.
     *  @param  bucket      The bucket where the stream is to be written.
     *  @param  key         The name of the file in that bucket.
     */
    public S3OutputStream(AmazonS3 client, String bucket, String key)
    {
        this(client, bucket, key, new ObjectMetadata(), 5 * 1024 * 1024);
    }


    /**
     *  Constructs an instance that uploads chunks inline, with the default part
     *  size (5 MB) and specified object metadata.
     *  <p>
     *  Uploads using this constructor are limited to 50GB; if you need to
     *  upload more data, you must specify a larger part size.
     *
     *  @param  client      The S3 client that will be used for uploading.
     *  @param  bucket      The bucket where the stream is to be written.
     *  @param  key         The name of the file in that bucket.
     *  @param  metadata    Object metadata. This can be used to provide content
     *                      type or caller-defined headers. Any provided content
     *                      length is ignored.
     */
    public S3OutputStream(AmazonS3 client, String bucket, String key, ObjectMetadata metadata)
    {
        this(client, bucket, key, metadata, 5 * 1024 * 1024);
    }


    /**
     *  Constructs an instance that uploads chunks inline, with the specified part
     *  size (5 MB) and object metadata. Part size must be at least 5MB.
     *
     *  @param  client      The S3 client that will be used for uploading.
     *  @param  bucket      The bucket where the stream is to be written.
     *  @param  key         The name of the file in that bucket.
     *  @param  metadata    Object metadata. This can be used to provide content
     *                      type or caller-defined headers. Any provided content
     *                      length is ignored.
     *  @param  partSize    Number of bytes to upload in a chunk. Must be >= 5 MB.
     */
    public S3OutputStream(AmazonS3 client, String bucket, String key, ObjectMetadata metadata, int partSize)
    {
        this(client, null, bucket, key, metadata, partSize);
    }


    /**
     *  Constructs an instance that uploads chunks on the specified threadpool,
     *  with the specified part size (5 MB) and object metadata.
     *  <p>
     *  Warning: this variant does not limit the number of chunks waiting for
     *  upload. If you produce faster than you can upload, you may run out of
     *  memory.
     *
     *  @param  client          The S3 client that will be used for uploading.
     *  @param  threadpool      Used to execute upload tasks.
     *  @param  bucket          The bucket where the stream is to be written.
     *  @param  key             The name of the file in that bucket.
     *  @param  metadata        Object metadata. This can be used to provide content
     *                          type or caller-defined headers. Any provided content
     *                          length is ignored.
     *  @param  partSize        Number of bytes to upload in a chunk. Must be >= 5 MB.
     */
    public S3OutputStream(AmazonS3 client, ExecutorService threadpool, String bucket, String key, ObjectMetadata metadata, int partSize)
    {
        this(client, threadpool, Integer.MAX_VALUE, bucket, key, metadata, partSize);
    }


    /**
     *  Constructs an instance that uploads chunks on the specified threadpool,
     *  with the specified part size (5 MB) and object metadata, limiting the
     *  number of outstanding chunks.
     *
     *  @param  client          The S3 client that will be used for uploading.
     *  @param  threadpool      Used to execute upload tasks.
     *  @param  maxOutstanding  The maximum number of uncompleted upload tasks
     *                          that are permitted.
     *  @param  bucket          The bucket where the stream is to be written.
     *  @param  key             The name of the file in that bucket.
     *  @param  metadata        Object metadata. This can be used to provide content
     *                          type or caller-defined headers. Any provided content
     *                          length is ignored.
     *  @param  partSize        Number of bytes to upload in a chunk. Must be >= 5 MB.
     */
    public S3OutputStream(AmazonS3 client, ExecutorService threadpool, int maxOutstanding, String bucket, String key, ObjectMetadata metadata, int partSize)
    {
        this.client = client;
        this.threadpool = threadpool;
        this.maxOutstanding = maxOutstanding;
        this.bucket = bucket;
        this.key = key;
        this.metadata = metadata;
        this.partSize = partSize;
        this.buffer = new ExposedByteArrayOutputStream(partSize);
    }

//----------------------------------------------------------------------------
//  OutputStream implementation
//----------------------------------------------------------------------------

    /**
     *  Closes the stream, writing the last segment and completing the upload.
     *  No-op if the stream has been aborted.
     */
    @Override
    public void close()
    throws IOException
    {
        if (buffer == null)
            return;

        if (upload == null)
        {
            logger.debug("buffer size is " + buffer.size() + "; using PutObject");
            int uploadSize = buffer.size();
            ByteArrayInputStream uploadChunk = new ByteArrayInputStream(buffer.get(), 0, uploadSize);
            metadata.setContentLength(uploadSize);
            client.putObject(bucket, key, uploadChunk, metadata);
        }
        else
        {
            writePart(true);
            upload.complete();
        }

        buffer = null;
    }


    /**
     *  This has no effect; the underlying stream writes data as it reaches the
     *  predefined buffer size.
     */
    @Override
    public void flush()
    throws IOException
    {
        // no-op
    }


    /**
     *  Writes a single byte to the buffer. Will trigger an upload if the buffer
     *  size exceeds configured part size.
     */
    @Override
    public void write(int b)
    throws IOException
    {
        if (buffer == null)
            return;

        buffer.write(b);
        optWritePart();
    }


    /**
     *  Writes an array to the buffer. Will trigger an upload if the buffer size exceeds
     *  configured part size.
     *  <p>
     *  Warning: the buffer will be expanded if necessary to contain the entire array.
     */
    @Override
    public void write(byte[] b)
    throws IOException
    {
        if (buffer == null)
            return;

        buffer.write(b);
        optWritePart();
    }


    /**
     *  Writes a sub-array array to the buffer. Will trigger an upload if the buffer size
     *  exceeds  configured part size.
     *  <p>
     *  Warning: the buffer will be expanded if necessary to contain the entire sub-array.
     */
    @Override
    public void write(byte[] b, int off, int len)
    throws IOException
    {
        if (buffer == null)
            return;

        buffer.write(b, off, len);
        optWritePart();
    }

//----------------------------------------------------------------------------
//  Other Public Methods
//----------------------------------------------------------------------------

    /**
     *  Aborts the multi-part upload.
     */
    public void abort()
    {
        buffer = null;
        if (upload != null)
        {
            upload.abort();
        }
    }

//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  This subclass allows access to the actual buffer, so that we don't
     *  need to duplicate it when writing to S3. Note that we allocate a bit
     *  more than is needed, to avoid unnecessary expansion.
     */
    private static class ExposedByteArrayOutputStream
    extends ByteArrayOutputStream
    {
        private int partSize;

        public ExposedByteArrayOutputStream(int partSize)
        {
            super((int)(partSize * 1.2));
            this.partSize = partSize;
        }

        public byte[] get()
        {
            return buf;
        }

        /**
         *  Resets the stream to "remove" the bytes corresponding to an uploaded
         *  part while preserving bytes written after that part.
         */
        @Override
        public void reset()
        {
            int preserveCount = count - partSize;
            if (preserveCount <= 0)
            {
                super.reset();
                return;
            }

            System.arraycopy(buf, partSize, buf, 0, preserveCount);
            count = preserveCount;
        }
    }


    private void optWritePart()
    {
        while (buffer.size() >= partSize)
        {
            if (upload == null)
            {
                upload = new MultipartUpload(client, threadpool, maxOutstanding);
                upload.begin(bucket, key, metadata);
            }
            writePart(false);
        }
    }


    private void writePart(boolean isLast)
    {
        int uploadSize = Math.min(partSize, buffer.size());
        if (uploadSize > 0)
        {
            upload.uploadPart(buffer.get(), 0, uploadSize, isLast);
            buffer.reset();
        }
    }
}
