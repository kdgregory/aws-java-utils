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
import java.util.List;

import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.model.*;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.*;


/**
 *  Utility functions for working with IAM and STS.
 */
public class IAMUtil
{
    /**
     *  Returns the current user's AWS account ID, null if unable to retrieve it
     *  (this indicates that the invoking user does not have valid credentials
     *  or is unable to connect to the Internet).
     */
    public static String retrieveAccountId(AWSSecurityTokenService stsClient)
    {
        try
        {
            GetCallerIdentityRequest request = new GetCallerIdentityRequest();
            GetCallerIdentityResult response = stsClient.getCallerIdentity(request);
            return response.getAccount();
        }
        catch (Exception ignored)
        {
            return null;
        }
    }


    /**
     *  Retrieves all roles for the account, internally handling pagination.
     */
    public static List<Role> retrieveAllRoles(AmazonIdentityManagement iamClient)
    {
        List<Role> result = new ArrayList<>();

        ListRolesRequest request = new ListRolesRequest();
        ListRolesResult response = null;
        do
        {
            response = iamClient.listRoles(request);
            result.addAll(response.getRoles());
            request.setMarker(response.getMarker());
        } while ((response.isTruncated() != null) && response.isTruncated().booleanValue());

        return result;
    }

}
