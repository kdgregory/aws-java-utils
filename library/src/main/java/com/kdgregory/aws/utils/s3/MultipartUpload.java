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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;


/**
 *  A class for performing multi-part uploads, optionally using a threadpool.
 *  <p>
 *  Each instance of this class can perform a single upload at a time. Instances
 *  may be reused after completing or aborting an upload. Multiple instances may
 *  share the S3 client and threadpool (which may also be used elsewhere in the
 *  program).
 *  <p>
 *  To upload an object:
 *  <ol>
 *  <li> Call {@link #begin}, passing the bucket name, key, and optional metadata.
 *  <li> Call {@link #uploadPart}, passing a buffer containing a chunk of the data.
 *       All chunks but the last must be the same size, and must be at least 5MB.
 *  <li> Call {@link #complete} to finish the upload. When using a threadpool, this
 *       method will block until all chunks have been uploaded.
 *  </ol>
 *
 *  You must catch any exceptions and call {@link #abort}, otherwise you'll end up
 *  paying for uncompleted uploads. Note that exceptions may be thrown by both
 *  <code>uploadPart()</code> and <code>complete()</code>.
 *  <p>
 *  <strong>Warning:</strong>
 *  If you produce data faster than it can be uploaded, and use a threadpool with
 *  an unbounded input queue, you might run out of memory. To avoid this, you can
 *  construct the instance with a maximum number of outstanding chunks. If you
 *  reach this number, then <code>uploadPart()</code> will block until chunks have
 *  been uploaded.
 *  <p>
 *  This class is not thread-safe. All methods must be called from the same thread,
 *  and the caller is responsible for synchronizing access. In particular, calls to
 *  <code>uploadPart()</code> from multiple threads may corrupt internal structures
 *  and/or upload parts out of order.
 */
public class MultipartUpload
{
    private Log logger = LogFactory.getLog(getClass());

    private AmazonS3 client;
    private ExecutorService executor;
    private int maxOutstandingTasks = Integer.MAX_VALUE;

    private String bucket;
    private String key;

    private String uploadId;
    private int partNumber;
    private List<Future<PartETag>> futures;


    /**
     *  Constructs an instance that performs the upload on the calling thread.
     */
    public MultipartUpload(AmazonS3 client)
    {
        this.client = client;
    }


    /**
     *  Constructs an instance that performs uploads using the provided threadpool.
     */
    public MultipartUpload(AmazonS3 client, ExecutorService executor)
    {
        this(client);
        this.executor = executor;
    }


    /**
     *  Constructs an instance that performs uploads using the provided threadpool,
     *  limiting the number of outstanding tasks.
     */
    public MultipartUpload(AmazonS3 client, ExecutorService executor, int maxOutstandingTasks)
    {
        this(client, executor);
        this.maxOutstandingTasks = maxOutstandingTasks;
    }


    /**
     *  Begins the upload, with default object metadata.
     */
    public void begin(String bucketName, String keyName)
    {
        begin(bucketName, keyName, new ObjectMetadata());
    }


    /**
     *  Begins the upload, with provided object metadata.
     */
    public void begin(String bucketName, String keyName, ObjectMetadata metadata)
    {
        bucket = bucketName;
        key = keyName;

        logger.debug("initiating multi-part upload for s3://" + bucket + "/" + key);

        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, key, metadata);
        InitiateMultipartUploadResult response = client.initiateMultipartUpload(request);

        uploadId = response.getUploadId();
        partNumber = 0; // note: part numbers start at 1, must pre-increment for each part
        futures = new ArrayList<>();

        logger.debug("initiated multi-part upload for s3://" + bucket + "/" + key + "; upload ID is " + uploadId);
    }


    /**
     *  Uploads a part. This will either dispatch a task to the threadpool or execute
     *  the upload inline.
     *
     *  @param data         The chunk to upload. Must be >= 5 MB, except for last chunk.
     *                      Will be defensively copied, allowing caller to modify array
     *                      while chunk is awaiting upload.
     *  @param isLastPart   If <code>true</code> signifies that this is the last part of
     *                      the upload.
     */
    public void uploadPart(byte[] data, boolean isLastPart)
    {
        uploadPart(data, 0, data.length, isLastPart);
    }


    /**
     *  Uploads a part from within a larger buffer. This will either dispatch a task to the
     *  threadpool or execute the upload inline.
     *  <p>
     *  Note: when using a threadpool, the provided buffer is copied, and may be modified
     *  by the caller while the task is outstanding. When executing inline, the buffer is
     *  not copied, and must not be modified by another thread until the part is uploaded.
     *
     *  @param data         Contains the part to upload.
     *  @param off          Offset of part within passed array.
     *  @param len          Length of part within passed array.
     *  @param isLastPart   If <code>true</code> signifies that this is the last part of
     *                      the upload.
     */
    public void uploadPart(byte[] data, int off, int len, boolean isLastPart)
    {
        final int localPartNumber = ++partNumber;

        ByteArrayInputStream dataStream;
        if (executor != null)
        {
            byte[] localBuf = new byte[len];
            System.arraycopy(data, off, localBuf, 0, len);
            dataStream = new ByteArrayInputStream(localBuf);
        }
        else
        {
            dataStream = new ByteArrayInputStream(data, off, len);
        }

        final UploadPartRequest request = new UploadPartRequest()
                                          .withBucketName(bucket)
                                          .withKey(key)
                                          .withUploadId(uploadId)
                                          .withPartNumber(localPartNumber)
                                          .withPartSize(len)
                                          .withInputStream(dataStream)
                                          .withLastPart(isLastPart);

        logger.debug("submitting part " + localPartNumber + " for s3://" + bucket + "/" + key);

        Callable<PartETag> callable = new Callable<PartETag>()
            {
                @Override
                public PartETag call() throws Exception
                {
                    logger.debug("uploading part " + localPartNumber + " for s3://" + bucket + "/" + key);
                    UploadPartResult response = client.uploadPart(request);
                    String etag = response.getETag();
                    logger.debug("completed part " + localPartNumber + " for s3://" + bucket + "/" + key + "; etag is " + etag);
                    return new PartETag(localPartNumber, etag);
                }
            };

        if (executor != null)
        {
            waitForTaskSlot();
            futures.add(executor.submit(callable));
        }
        else
        {
            // we call <code>unwrap()</code> here in order to fail fast if there was an exception,
            //  but we also add the future to the list so that complete() doesn't need special code
            Future<PartETag> future = executeInline(callable);
            unwrap(future);
            futures.add(future);
        }
    }


    /**
     *  Completes the upload, waiting for all outstanding tasks.
     */
    public void complete()
    {
        logger.debug("waiting for upload tasks of s3://" + bucket + "/" + key);
        List<PartETag> partTags = new ArrayList<>();
        for (Future<PartETag> future : futures)
        {
            partTags.add(unwrap(future));
        }

        logger.debug("completing multi-part upload for s3://" + bucket + "/" + key);
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest()
                                                  .withBucketName(bucket)
                                                  .withKey(key)
                                                  .withUploadId(uploadId)
                                                  .withPartETags(partTags);
        client.completeMultipartUpload(request);
        logger.debug("completed multi-part upload for s3://" + bucket + "/" + key);
    }


    /**
     *  Aborts the upload. Attempts to cancel any outstanding tasks.
     *
     *  This runs on the calling thread.
     */
    public void abort()
    {
        logger.debug("aborting multi-part upload for s3://" + bucket + "/" + key);

        for (Future<PartETag> future : futures)
        {
            future.cancel(true);
        }

        AbortMultipartUploadRequest request = new AbortMultipartUploadRequest(bucket, key, uploadId);
        client.abortMultipartUpload(request);
        logger.debug("aborted multi-part upload for s3://" + bucket + "/" + key);
    }


    /**
     *  Returns the number of outstanding (submitted but not complete) tasks.
     */
    public int outstandingTaskCount()
    {
        return outstandingTasks().size();
    }


//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

    /**
     *  Returns all tasks that are not yet done.
     */
    private List<Future<PartETag>> outstandingTasks()
    {
        List<Future<PartETag>> result = new ArrayList<>();
        for (Future<PartETag> future : futures)
        {
            if (!future.isDone())
                result.add(future);
        }
        return result;
    }


    /**
     *  Waits until the outstanding tasks are below the limit.
     */
    private void waitForTaskSlot()
    {
        while (true)
        {
            List<Future<PartETag>> tasks = outstandingTasks();
            if (tasks.isEmpty() || tasks.size() < maxOutstandingTasks)
                return;
            unwrap(tasks.get(0));
        }
    }


    /**
     *  Common code for unwrapping an upload Future, throwing any exception as
     *  a RuntimeException.
     */
    private PartETag unwrap(Future<PartETag> future)
    {
        try
        {
            return future.get();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(String.format("failed to complete upload task for s3://%s/%s"), e.getCause());
        }
        catch (Exception e)
        {
            throw new RuntimeException(String.format("failed to complete upload task for s3://%s/%s"), e);
        }
    }


    /**
     *  Executes a <code>Callable</code> inline, and returns the <code>Future</code>
     *  representing its execution.
     *
     *  // TODO - move to KDGCommons
     */
    private static <T> Future<T> executeInline(Callable<T> callable)
    {
        try
        {
            final T v = callable.call();
            return new Future<T>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning)
                {
                    return false;
                }

                @Override
                public boolean isCancelled()
                {
                    return false;
                }

                @Override
                public boolean isDone()
                {
                    return true;
                }

                @Override
                public T get() throws InterruptedException, ExecutionException
                {
                    return v;
                }

                @Override
                public T get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException
                {
                    return v;
                }
            };
        }
        catch (final Throwable t)
        {
            return new Future<T>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning)
                {
                    return false;
                }

                @Override
                public boolean isCancelled()
                {
                    return false;
                }

                @Override
                public boolean isDone()
                {
                    return true;
                }

                @Override
                public T get() throws InterruptedException, ExecutionException
                {
                    throw new ExecutionException(t);
                }

                @Override
                public T get(long timeout, TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException
                {
                    throw new ExecutionException(t);
                }
            };
        }
    }
}
