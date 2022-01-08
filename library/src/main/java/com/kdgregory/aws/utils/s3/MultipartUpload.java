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
 *  Each upload uses a separate instance of this class, with shared S3 client
 *  and (if used) threadpool. After construction call {@link #begin} to get
 *  the multi-part upload token from S3. Then call {@link #uploadPart} for
 *  each block of the file. All blocks but the last must be the same size, and
 *  must be 5 MB or larger. Finally, call {@link #complete}. When using a
 *  threadpool, this method will wait for all parts to finish uploading.
 *  <p>
 *  Must catch any exceptions and call {@link #abort}, otherwise you'll end up
 *  paying for uncompleted uploads. Note that, when using a threadpool, upload
 *  exceptions will be reported in <code>complete()</code>.
 *  <p>
 *  <strong>Warning:</strong>
 *  If you produce data faster than it can be uploaded, and use a threadpool with
 *  and unbounded input queue, then you can run out of memory.
 *
 *  // TODO - add method to return number of uncompleted tasks
 *  // TODO - support object metadata
 */
public class MultipartUpload
{
    private Log logger = LogFactory.getLog(getClass());

    private AmazonS3 client;
    private ExecutorService executor;

    private String bucket;
    private String key;

    private String uploadId;
    private int partNumber = 0; // note: part numbers start at 1, must increment before submitting
    private List<Future<PartETag>> futures = new ArrayList<>();


    /**
     *  Constructs an instance that performs the upload on the calling thread.
     */
    public MultipartUpload(AmazonS3 client, String bucket, String key)
    {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
    }


    /**
     *  Constructs an instance with performs the upload using the provided threadpool.
     */
    public MultipartUpload(AmazonS3 client, String bucket, String key, ExecutorService executor)
    {
        this(client, bucket, key);
        this.executor = executor;
    }


    /**
     *  Begins the upload, retrieving the upload token from S3.
     *
     *  This runs on the calling thread.
     */
    public void begin()
    {
        logger.debug("initiating multi-part upload for s3://" + bucket + "/" + key);

        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucket, key);
        InitiateMultipartUploadResult response = client.initiateMultipartUpload(request);

        uploadId = response.getUploadId();
        logger.debug("initiated multi-part upload for s3://" + bucket + "/" + key + "; upload ID is " + uploadId);
    }


    /**
     *  Uploads a part. This will either dispatch a task to the threadpool or execute
     *  the upload inline.
     *
     *  @param data         The chunk to upload. Must be >= 5 MB, except for last chunk.
     *                      Will be defensively copied, allowing caller to refill array.
     *  @param isLastPart   If <code>true</code> signifies that this is the last part of
     *                      the upload.
     */
    public synchronized void uploadPart(byte[] data, boolean isLastPart)
    {
        final int localPartNumber = ++partNumber;
        final UploadPartRequest request = new UploadPartRequest()
                                          .withBucketName(bucket)
                                          .withKey(key)
                                          .withUploadId(uploadId)
                                          .withPartNumber(localPartNumber)
                                          .withPartSize(data.length)
                                          .withInputStream(new ByteArrayInputStream(data.clone()))
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
     *
     *  This runs on the calling thread.
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


//----------------------------------------------------------------------------
//  Internals
//----------------------------------------------------------------------------

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
