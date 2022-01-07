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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;


/**
 *  A class for performing concurrent multi-part uploads.
 *
 *  The sample main generates random data and uploads it in 10 MB chunks, to
 *  verify operation.
 */
public class ConcurrentMultipartUpload
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
     *  Constructs an instance with provided threadpool.
     */
    public ConcurrentMultipartUpload(AmazonS3 client, String bucket, String key, ExecutorService executor)
    {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
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
     *  Uploads a part. All parts except the last must be the same size.
     *
     *  This submits a task to upload the part; the provided threadpool is responsible
     *  for scheduling that task.
     */
    public synchronized void uploadPart(byte[] data, boolean isLastPart)
    {
        partNumber++;
        logger.debug("submitting part " + partNumber + " for s3://" + bucket + "/" + key);

        final UploadPartRequest request = new UploadPartRequest()
                                          .withBucketName(bucket)
                                          .withKey(key)
                                          .withUploadId(uploadId)
                                          .withPartNumber(partNumber)
                                          .withPartSize(data.length)
                                          .withInputStream(new ByteArrayInputStream(data))
                                          .withLastPart(isLastPart);

        futures.add(
            executor.submit(new Callable<PartETag>()
            {
                @Override
                public PartETag call() throws Exception
                {
                    int localPartNumber = request.getPartNumber();
                    logger.debug("uploading part " + localPartNumber + " for s3://" + bucket + "/" + key);
                    UploadPartResult response = client.uploadPart(request);
                    String etag = response.getETag();
                    logger.debug("uploaded part " + localPartNumber + " for s3://" + bucket + "/" + key + "; etag is " + etag);
                    return new PartETag(localPartNumber, etag);
                }
            }));
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
            try
            {
                partTags.add(future.get());
            }
            catch (Exception e)
            {
                throw new RuntimeException(String.format("failed to complete upload task for s3://%s/%s"), e);
            }
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
}
