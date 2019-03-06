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

package com.kdgregory.aws.utils.testhelpers.mocks;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

import net.sf.kdgcommons.util.Counters;



/**
 *  Common superclass for mock objects. This class implements the invocation handler
 *  and tracks invocation parameters. Subclasses should put "friendly" wrappers around
 *  the argument-retrieval functions.
 *  <p>
 *  This class is based on <code>net.sf.kdgcommons.test.SelfMock</code>, and may
 *  become the next version of that class.
 */
public abstract class AbstractMock<T>
implements InvocationHandler
{
    private Class<T> mockedClass;

    public volatile Thread lastInvocationThread;
    public volatile Object[] lastInvocationArgs;

    private ConcurrentHashMap<String,List<Object[]>> invocationArgs = new ConcurrentHashMap<String,List<Object[]>>();
    private Counters<String> invocationCounts = new Counters<String>();


    public AbstractMock(Class<T> mockedClass)
    {
        this.mockedClass = mockedClass;
    }


    /**
     *  Returns a new instance of the client interface, refering to this class as the
     *  invocation handler. Multiple calls to this method return different proxy
     *  objects backed by the same mock instance.
     */
    public T getInstance()
    {
        return mockedClass.cast(
                Proxy.newProxyInstance(
                    this.getClass().getClassLoader(),
                    new Class[] { mockedClass },
                    this));
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable
    {
        try
        {
            String methodName = method.getName();
            Method selfMethod = getClass().getMethod(methodName, method.getParameterTypes());
            selfMethod.setAccessible(true);

            lastInvocationThread = Thread.currentThread();
            lastInvocationArgs = args;

            List<Object[]> perMethodArgs = invocationArgs.get(methodName);
            if (perMethodArgs == null)
            {
                // gotta do this for correct synchronization
                invocationArgs.putIfAbsent(methodName, Collections.synchronizedList(new ArrayList<Object[]>()));
                perMethodArgs = invocationArgs.get(methodName);
            }
            perMethodArgs.add(args);
            invocationCounts.increment(methodName);
            return selfMethod.invoke(this, args);
        }
        catch (NoSuchMethodException ex)
        {
            throw new UnsupportedOperationException("mock does not implement method: " + method.getName()
                                                    + "(" + Arrays.asList(method.getParameterTypes()) + ")");
        }
        catch (SecurityException ex)
        {
            throw new RuntimeException("security exception when invoking: " + method.getName(), ex);
        }
        catch (IllegalAccessException ex)
        {
            throw new RuntimeException("illegal access exception when invoking: " + method.getName(), ex);
        }
        catch (InvocationTargetException ex)
        {
            throw ex.getCause();
        }
    }

//----------------------------------------------------------------------------
//  Accessor API
//----------------------------------------------------------------------------

    /**
     *  Returns the number of times that the method was invoked.
     */
    public int getInvocationCount(String methodName)
    {
        return (int)invocationCounts.getLong(methodName);
    }


    /**
     *  Returns the complete set of arguments from the last invocation.
     */
    public Object[] getLastInvocationArgs()
    {
        return lastInvocationArgs;
    }


    /**
     *  Returns the complete set of arguments from the Nth invocation of
     *  a given method.
     */
    public Object[] getInvocationArgs(String methodName, int index)
    {
        return invocationArgs.get(methodName).get(index);
    }


    /**
     *  Returns the first argument from the last invocation, casting it to the
     *  specified type. This is a convenience for most AWS SDK methods, which
     *  take a single "request" argument. Will throw if passed incorrect class
     *  or there wasn't an argument.
     */
    public <ArgType> ArgType getLastInvocationArgAs(int index, Class<ArgType> klass)
    {
        return klass.cast(lastInvocationArgs[index]);
    }


    /**
     *  Returns the first argument from the specified invocation, casting it to the
     *  specified type.
     */
    public <ArgType> ArgType getInvocationArgAs(String methodName, int callIndex, int argIndex, Class<ArgType> klass)
    {
        return klass.cast(getInvocationArgs(methodName, callIndex)[argIndex]);
    }


    /**
     *  Returns the thread on which the last invocation was made. This is typically used
     *  to verify that the invocation was not made on the test thread.
     */
    public Thread getLastInvocationThread()
    {
        return lastInvocationThread;
    }

//----------------------------------------------------------------------------
//  Assertions
//----------------------------------------------------------------------------

    /**
     *  Asserts that the last invocation did not occur on the current thread
     *  (verifies background operation).
     */
    public void assertLastInvocationNotOnCurrentThread()
    {
        assertFalse("invocation was not on current thread", Thread.currentThread() == lastInvocationThread);
    }


    /**
     *  Asserts the number of times that a particular method was invoked.
     */
    public void assertInvocationCount(String methodName, int expected)
    {
        assertInvocationCount(null, methodName, expected);
    }


    /**
     *  Asserts the number of times that a particular method was invoked. The
     *  assertion method will include the provided message suffix (if not null),
     *  which may be used to provide additional information about the assertion.
     */
    public void assertInvocationCount(String messageSuffix, String methodName, int expected)
    {
        String message = "invocation count: " + methodName;
        if (messageSuffix != null)
            message += ", " + messageSuffix;

        assertEquals(message, expected, getInvocationCount(methodName));
    }
}