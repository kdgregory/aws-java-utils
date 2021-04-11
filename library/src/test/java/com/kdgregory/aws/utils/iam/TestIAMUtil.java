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

package com.kdgregory.aws.utils.iam;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import net.sf.kdgcommons.collections.CollectionUtil;
import net.sf.kdgcommons.test.SelfMock;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.*;

import com.kdgregory.aws.utils.testhelpers.Log4JCapturingAppender;


public class TestIAMUtil
{
    private Log4JCapturingAppender logCapture;

//----------------------------------------------------------------------------
//  Per-test boilerplate
//----------------------------------------------------------------------------

    @Before
    public void setUp()
    {
        logCapture = Log4JCapturingAppender.getInstance();
        logCapture.reset();
    }

//----------------------------------------------------------------------------
//  Testcases
//----------------------------------------------------------------------------

    @Test
    public void testRetrieveAccountIdHappyPath() throws Exception
    {
        final String accountId = "123456789012";
        STSClientMock mock = new STSClientMock(accountId);

        assertEquals(accountId, IAMUtil.retrieveAccountId(mock.getInstance()));
    }


    @Test
    public void testRetrieveAccountIdSadPath() throws Exception
    {
        STSClientMock mock = new STSClientMock(null);

        assertNull(IAMUtil.retrieveAccountId(mock.getInstance()));
    }


    @Test
    public void testRetrieveRolesUnpaginated() throws Exception
    {
        IAMClientMock mock = new IAMClientMock("123456789012")
                             .withKnownRoles("foo", "bar", "baz");

        List<Role> result = IAMUtil.retrieveAllRoles(mock.getInstance());

        assertEquals("number of roles returned",        3,      result.size());
        assertEquals("number of calls to listRoles()",  1,      mock.getInvocationCount("listRoles"));

        Set<String> returnedRoleNames = new HashSet<>();
        for (Role role : result)
        {
            returnedRoleNames.add(role.getRoleName());
        }
        assertEquals("returned role names", CollectionUtil.asSet("foo", "bar", "baz"), returnedRoleNames);
    }


    @Test
    public void testRetrieveRolesPaginated() throws Exception
    {
        IAMClientMock mock = new IAMClientMock("123456789012")
                             .withKnownRoles("foo", "bar", "baz")
                             .withPageSize(2);

        List<Role> result = IAMUtil.retrieveAllRoles(mock.getInstance());

        assertEquals("number of roles returned",        3,      result.size());
        assertEquals("number of calls to listRoles()",  2,      mock.getInvocationCount("listRoles"));

        Set<String> returnedRoleNames = new HashSet<>();
        for (Role role : result)
        {
            returnedRoleNames.add(role.getRoleName());
        }
        assertEquals("returned role names", CollectionUtil.asSet("foo", "bar", "baz"), returnedRoleNames);
    }


    @Test
    public void testRetrieveRolesEmpty() throws Exception
    {
        // this will never happen in real life, but we should verify we don't do anything stupid
        IAMClientMock mock = new IAMClientMock("123456789012");

        List<Role> result = IAMUtil.retrieveAllRoles(mock.getInstance());

        assertEquals("number of roles returned",        0,      result.size());
        assertEquals("number of calls to listRoles()",  1,      mock.getInvocationCount("listRoles"));
    }

//----------------------------------------------------------------------------
//  Helpers
//----------------------------------------------------------------------------

    /**
     *  A mock object that supports only the operations tested in this module.
     */
    public class IAMClientMock
    extends SelfMock<AmazonIdentityManagement>
    {
        private String testAccountId;
        private int pageSize = Integer.MAX_VALUE / 2;   // a very large number that avoids wrap-around
        private List<Role> knownRoles = new ArrayList<>();

        public IAMClientMock(String testAccountId)
        {
            super(AmazonIdentityManagement.class);
            this.testAccountId = testAccountId;
        }

        // optional configuration

        public IAMClientMock withKnownRoles(String... roleNames)
        {
            for (String roleName : roleNames)
            {
                Role role = new Role()
                            .withRoleName(roleName)
                            .withArn("arn:aws:iam::" + testAccountId + ":role/" + roleName);
                knownRoles.add(role);
            }
            return this;
        }

        public IAMClientMock withPageSize(int value)
        {
            this.pageSize = value;
            return this;
        }

        // mock implementations

        public ListRolesResult listRoles(ListRolesRequest request)
        {
            int startIdx = (request.getMarker() != null)
                         ? Integer.valueOf(request.getMarker())
                         : 0;
            int endIdx = Math.min(knownRoles.size(), startIdx + pageSize);
            boolean truncated = endIdx < knownRoles.size();
            String marker = truncated ? String.valueOf(endIdx) : null;

            return new ListRolesResult()
                   .withRoles(Collections.unmodifiableList(knownRoles.subList(startIdx, endIdx)))
                   .withIsTruncated(Boolean.valueOf(truncated))
                   .withMarker(marker);
        }

    }


    /**
     *  A mock object that supports only the operations tested in this module.
     */
    public class STSClientMock
    extends SelfMock<AWSSecurityTokenService>
    {
        private String testAccountId;


        public STSClientMock(String testAccountId)
        {
            super(AWSSecurityTokenService.class);
            this.testAccountId = testAccountId;
        }

        // mock implementations

        public GetCallerIdentityResult getCallerIdentity(GetCallerIdentityRequest request)
        {
            if (testAccountId != null)
            {
                return new GetCallerIdentityResult().withAccount(testAccountId);
            }
            else
            {
                // determined by experimentation; does not include all details
                throw new AWSSecurityTokenServiceException("invalid token");
            }
        }
    }
}
