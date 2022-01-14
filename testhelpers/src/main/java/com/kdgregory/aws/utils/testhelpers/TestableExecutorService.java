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

package com.kdgregory.aws.utils.testhelpers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 *  A "threadpool" that runs tasks inline and gives insight into its operations.
 *
 *  // TODO - move this into KDGCommons
 */
public class TestableExecutorService
extends AbstractExecutorService
{
    private List<Callable<?>> submittedTasks = new ArrayList<>();
    private List<Callable<?>> completedTasks = new ArrayList<>();

    public int getSubmitCount()
    {
        return submittedTasks.size();
    }

    public int getCompleteCount()
    {
        return completedTasks.size();
    }

    @Override
    public <T> Future<T> submit(Callable<T> task)
    {
        submittedTasks.add(task);
        return new TestableFuture<T>(task);
    }

    @Override
    public void execute(Runnable command)
    {
        throw new UnsupportedOperationException("should not be submitting Runnables");
    }

    @Override
    public List<Runnable> shutdownNow()
    {
        throw new UnsupportedOperationException("should not be shutting down threadpool");
    }

    @Override
    public void shutdown()
    {
        throw new UnsupportedOperationException("should not be shutting down threadpool");
    }

    @Override
    public boolean isTerminated()
    {
        return false;
    }

    @Override
    public boolean isShutdown()
    {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
        throw new UnsupportedOperationException("should not be awaiting threadpool termination");
    }


    private class TestableFuture<T>
    implements Future<T>
    {
        private Callable<T> callable;
        private T result;
        private boolean isCanceled;
        private boolean isDone;

        public TestableFuture(Callable<T> callable)
        {
            this.callable = callable;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            isCanceled = true;
            return true;
        }

        @Override
        public boolean isCancelled()
        {
            return isCanceled;
        }

        @Override
        public boolean isDone()
        {
            return isDone;
        }

        @Override
        public T get() throws InterruptedException, ExecutionException
        {
            if (isDone)
                return result;
            if (isCanceled)
                throw new CancellationException("called get() on canceled future");

            try
            {
                isDone = true;
                completedTasks.add(callable);
                result = callable.call();
                return result;
            }
            catch (Exception ex)
            {
                throw new ExecutionException(ex);
            }
        }

        @Override
        public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
        {
            return get();
        }
    }
}