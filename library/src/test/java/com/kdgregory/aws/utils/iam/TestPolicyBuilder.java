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

import org.w3c.dom.Document;

import org.junit.Test;
import static org.junit.Assert.*;

import static net.sf.kdgcommons.test.StringAsserts.*;

import net.sf.practicalxml.converter.JsonConverter;
import net.sf.practicalxml.converter.json.Json2XmlOptions;
import net.sf.practicalxml.junit.DomAsserts;
import net.sf.practicalxml.xpath.XPathWrapper;

import com.kdgregory.aws.utils.iam.PolicyBuilder;
import com.kdgregory.aws.utils.iam.PolicyBuilder.Condition;
import com.kdgregory.aws.utils.iam.PolicyBuilder.NotPrincipal;
import com.kdgregory.aws.utils.iam.PolicyBuilder.Principal;
import com.kdgregory.aws.utils.iam.PolicyBuilder.Statement;


public class TestPolicyBuilder
{

//----------------------------------------------------------------------------
//  Testcases
//
//  Note: these tests convert the JSON to XML so that we can apply assertions
//  based on XPath, which is a lot easier than trying to string-match JSON or
//  transform into some other representation such as a Map
//
//----------------------------------------------------------------------------

    @Test
    public void testEmptyPolicy() throws Exception
    {
        String json = new PolicyBuilder().build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",          "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertCount("statement count",   0,              dom, "/data/Statement/*");

        // the Id element does not have any restrictions on characters; this first assertion is
        // that we have something
        String id = new XPathWrapper("/data/Id").evaluateAsString(dom);
        assertRegex("generated policy ID (was: " + id + ")",
                    ".+",
                    id);

        // and we'll build a second policy to assert that the two values are not equal
        String json2 = new PolicyBuilder().build();
        Document dom2 = JsonConverter.convertToXml(json2, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);
        String id2 = new XPathWrapper("/data/Id").evaluateAsString(dom2);

        assertNotEquals("generated ID should be unique", id, id2);
    }


    @Test
    public void testExplicitId() throws Exception
    {
        String json = new PolicyBuilder().id("example").build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",          "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",               "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",   0,              dom, "/data/Statement/*");
    }


    @Test
    public void testSimpleAllow() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("bar")
                          .resources("argle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",              "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",                   "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",       1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",         "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",               "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",          1,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action",               "bar",          dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",        1,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",             "argle",        dom, "/data/Statement/Resource");
    }


    @Test
    public void testSimpleDeny() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .deny()
                          .actions("bar")
                          .resources("argle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",              "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",                   "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",       1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",         "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",               "Deny",         dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",          1,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action",               "bar",          dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",        1,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",             "argle",        dom, "/data/Statement/Resource");
    }


    @Test
    public void testMissingEffect() throws Exception
    {
        PolicyBuilder builder
                    = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .actions("bar")
                          .resources("argle")
                      );

        try
        {
            builder.build();
            fail("should have thrown with missing effect");
        }
        catch (IllegalArgumentException ex)
        {
            String msg = ex.getMessage();
            assertTrue("exception should mention missing effect (was: " + msg + ")", msg.contains("effect"));
        }
    }


    @Test
    public void testMissingSid() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .allow()
                          .actions("bar")
                          .resources("argle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",              "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",                   "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",       1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",         "",             dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",               "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",          1,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action",               "bar",          dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",        1,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",             "argle",        dom, "/data/Statement/Resource");
    }


    @Test
    public void testUnrestrictedPrincipal() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .principal(new Principal().all())
                          .actions("bar")
                          .resources("argle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",              "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",                   "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",       1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",         "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",               "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("principal count",       1,              dom, "/data/Statement/Principal");
        DomAsserts.assertEquals("principal value",      "*",            dom, "/data/Statement/Principal");
        DomAsserts.assertCount("action count",          1,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action",               "bar",          dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",        1,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",             "argle",        dom, "/data/Statement/Resource");
    }


    @Test
    public void testSpecifiedPrincipals() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .principal(new Principal()
                              .awsIdentities("arn:aws:iam::123456789012:root")
                              .awsIdentities("123456789012")
                              .federatedIdentities("cognito-identity.amazonaws.com")
                              .federatedIdentities("accounts.google.com")
                              .awsServices("elasticmapreduce.amazonaws.com")
                              .awsServices("datapipeline.amazonaws.com")
                              )
                          .actions("bar")
                          .resources("argle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        // note: ordering of principals isn't specified by grammar, but imposed by implementation

        DomAsserts.assertEquals("Version",              "2012-10-17",                       dom, "/data/Version");
        DomAsserts.assertEquals("ID",                   "example",                          dom, "/data/Id");
        DomAsserts.assertCount("statement count",       1,                                  dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",         "foo",                              dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",               "Allow",                            dom, "/data/Statement/Effect");
        DomAsserts.assertCount("principal count",       6,                                  dom, "/data/Statement/Principal/*");
        DomAsserts.assertEquals("principal 1",          "123456789012",                     dom, "/data/Statement/Principal/AWS[1]");
        DomAsserts.assertEquals("principal 2",          "arn:aws:iam::123456789012:root",   dom, "/data/Statement/Principal/AWS[2]");
        DomAsserts.assertEquals("principal 3",          "accounts.google.com",              dom, "/data/Statement/Principal/Federated[1]");
        DomAsserts.assertEquals("principal 4",          "cognito-identity.amazonaws.com",   dom, "/data/Statement/Principal/Federated[2]");
        DomAsserts.assertEquals("principal 5",          "datapipeline.amazonaws.com",       dom, "/data/Statement/Principal/Service[1]");
        DomAsserts.assertEquals("principal 6",          "elasticmapreduce.amazonaws.com",   dom, "/data/Statement/Principal/Service[2]");
        DomAsserts.assertCount("action count",          1,                                  dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action",               "bar",                              dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",        1,                                  dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",             "argle",                            dom, "/data/Statement/Resource");
    }


    @Test
    public void testNotPrincipal() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .principal(new NotPrincipal().all())
                          .actions("bar")
                          .resources("argle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",              "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",                   "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",       1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",         "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",               "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("not-principal count",   1,              dom, "/data/Statement/NotPrincipal");
        DomAsserts.assertEquals("not-principal value",  "*",            dom, "/data/Statement/NotPrincipal");
        DomAsserts.assertCount("action count",          1,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action",               "bar",          dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",        1,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",             "argle",        dom, "/data/Statement/Resource");
    }


    @Test
    public void testActions() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("bar")
                          .actions("baz", "biff")
                          .resources("argle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",          "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",               "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",   1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",     "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",           "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",      3,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action 1",         "bar",          dom, "/data/Statement/Action[1]");
        DomAsserts.assertEquals("action 2",         "baz",          dom, "/data/Statement/Action[2]");
        DomAsserts.assertEquals("action 2",         "biff",         dom, "/data/Statement/Action[3]");
        DomAsserts.assertCount("resource count",    1,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",         "argle",        dom, "/data/Statement/Resource");
    }


    @Test
    public void testNotActions() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .notActions("bar")
                          .notActions("baz", "biff")
                          .resources("argle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",          "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",               "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",   1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",     "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",           "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",      3,              dom, "/data/Statement/NotAction");
        DomAsserts.assertEquals("action 1",         "bar",          dom, "/data/Statement/NotAction[1]");
        DomAsserts.assertEquals("action 2",         "baz",          dom, "/data/Statement/NotAction[2]");
        DomAsserts.assertEquals("action 2",         "biff",         dom, "/data/Statement/NotAction[3]");
        DomAsserts.assertCount("resource count",    1,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",         "argle",        dom, "/data/Statement/Resource");
    }


    @Test
    public void testMissingActions() throws Exception
    {
        PolicyBuilder builder
                    = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .resources("argle")
                      );

        try
        {
            builder.build();
            fail("should have thrown with missing actions");
        }
        catch (IllegalArgumentException ex)
        {
            String msg = ex.getMessage();
            assertTrue("exception should mention missing action (was: " + msg + ")", msg.contains("action"));
        }
    }


    @Test
    public void testMixingActionsAndNotActions() throws Exception
    {
        PolicyBuilder builder
                    = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("foo")
                          .notActions("bar")
                          .resources("argle")
                      );

        try
        {
            builder.build();
            fail("should have thrown with both actions and not-actions");
        }
        catch (IllegalArgumentException ex)
        {
            String msg = ex.getMessage();
            assertTrue("exception should mention combined actions (was: " + msg + ")",
                       msg.contains("either actions or not-actions"));
        }
    }


    @Test
    public void testSingleResource() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("bar")
                          .resources("wargle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",          "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",               "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",   1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",     "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",           "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",      1,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action 1",         "bar",          dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",    1,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource",         "wargle",       dom, "/data/Statement/Resource");
    }


    @Test
    public void testMultipleResources() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("bar")
                          .resources("argle", "bargle")
                          .resources("wargle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",          "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",               "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",   1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",     "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",           "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",      1,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action 1",         "bar",          dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",    3,              dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource 1",       "argle",        dom, "/data/Statement/Resource[1]");
        DomAsserts.assertEquals("resource 2",       "bargle",       dom, "/data/Statement/Resource[2]");
        DomAsserts.assertEquals("resource 3",       "wargle",       dom, "/data/Statement/Resource[3]");
    }


    @Test
    public void testNotResources() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("bar")
                          .notResources("argle", "bargle")
                          .notResources("wargle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",              "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",                   "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",       1,              dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",         "foo",          dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",               "Allow",        dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",          1,              dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action 1",             "bar",          dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",        3,              dom, "/data/Statement/NotResource");
        DomAsserts.assertEquals("resource 1",           "argle",        dom, "/data/Statement/NotResource[1]");
        DomAsserts.assertEquals("resource 2",           "bargle",       dom, "/data/Statement/NotResource[2]");
        DomAsserts.assertEquals("resource 3",           "wargle",       dom, "/data/Statement/NotResource[3]");
    }


    @Test
    public void testMissingResources() throws Exception
    {
        PolicyBuilder builder
                    = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("bar")
                      );

        try
        {
            builder.build();
            fail("should have thrown when missing resources");
        }
        catch (IllegalArgumentException ex)
        {
            String msg = ex.getMessage();
            assertTrue("exception should mention missing resources (was: " + msg + ")", msg.contains("must specify resources"));
        }
    }


    @Test
    public void testMixingResourcesAndNotResources() throws Exception
    {
        PolicyBuilder builder
                    = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("bar")
                          .resources("argle")
                          .notResources("bargle")
                      );

        try
        {
            builder.build();
            fail("should have thrown when specifying both resources and not-resources");
        }
        catch (IllegalArgumentException ex)
        {
            String msg = ex.getMessage();
            assertTrue("exception should mention missing resources (was: " + msg + ")",
                       msg.contains("either resources or not-resources"));
        }
    }


    @Test
    public void testConditions() throws Exception
    {
        // note: example conditions are invalid; key names can't be converted to XML

        String json = new PolicyBuilder()
                      .id("example")
                      .statements(new Statement()
                          .sid("foo")
                          .allow()
                          .actions("bar")
                          .resources("argle")
                          .conditions(
                              new Condition("DateGreaterThan", "awsCurrentTime", "2013-08-16T12:00:00Z"),
                              new Condition("DateLessThan", "awsCurrentTime",    "2013-08-16T15:00:00Z"))
                          .conditions(
                              new Condition("IpAddress", "awsSourceIp", "192.0.2.0/24", "203.0.113.0/24"))
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",              "2012-10-17",           dom, "/data/Version");
        DomAsserts.assertEquals("ID",                   "example",              dom, "/data/Id");
        DomAsserts.assertCount("statement count",       1,                      dom, "/data/Statement");
        DomAsserts.assertEquals("statement id",         "foo",                  dom, "/data/Statement/Sid");
        DomAsserts.assertEquals("effect",               "Allow",                dom, "/data/Statement/Effect");
        DomAsserts.assertCount("action count",          1,                      dom, "/data/Statement/Action");
        DomAsserts.assertEquals("action 1",             "bar",                  dom, "/data/Statement/Action");
        DomAsserts.assertCount("resource count",        1,                      dom, "/data/Statement/Resource");
        DomAsserts.assertEquals("resource 1",           "argle",                dom, "/data/Statement/Resource");
        DomAsserts.assertCount("condition count",       3,                      dom, "/data/Statement/Condition/*");
        DomAsserts.assertEquals("condition 1",          "2013-08-16T12:00:00Z", dom, "/data/Statement/Condition/DateGreaterThan/awsCurrentTime");
        DomAsserts.assertEquals("condition 2",          "2013-08-16T15:00:00Z", dom, "/data/Statement/Condition/DateLessThan/awsCurrentTime");
        DomAsserts.assertEquals("condition 3a",         "192.0.2.0/24",         dom, "/data/Statement/Condition/IpAddress/awsSourceIp[1]");
        DomAsserts.assertEquals("condition 3b",         "203.0.113.0/24",       dom, "/data/Statement/Condition/IpAddress/awsSourceIp[2]");
    }


    @Test
    public void testRepeatedConditions() throws Exception
    {
        try
        {
            new PolicyBuilder()
            .id("example")
            .statements(new Statement()
                  .sid("foo")
                  .allow()
                  .actions("bar")
                  .resources("argle")
                  .conditions(
                      new Condition("DateGreaterThan", "awsCurrentTime", "2013-08-16T12:00:00Z"))
                  .conditions(
                      new Condition("DateGreaterThan", "awsCurrentTime",    "2013-08-16T15:00:00Z"))
            );
            fail("allowed repeated conditions with same key");
        }
        catch (IllegalArgumentException ex)
        {
            assertTrue("exception message indicates multiple conditions", ex.getMessage().contains("multiple conditions"));
            assertTrue("exception message indicates condition tpye",      ex.getMessage().contains("DateGreaterThan"));
        }

    }


    @Test
    public void testMultipleStatements() throws Exception
    {
        String json = new PolicyBuilder()
                      .id("example")
                      .statements(
                          new Statement()
                              .sid("stmt1")
                              .allow()
                              .actions("foo")
                              .resources("argle"),
                          new Statement()
                              .sid("stmt2")
                              .deny()
                              .actions("bar")
                              .resources("bargle")
                      ).build();
        Document dom = JsonConverter.convertToXml(json, "", Json2XmlOptions.ARRAYS_AS_REPEATED_ELEMENTS);

        DomAsserts.assertEquals("Version",                      "2012-10-17",   dom, "/data/Version");
        DomAsserts.assertEquals("ID",                           "example",      dom, "/data/Id");
        DomAsserts.assertCount("statement count",               2,              dom, "/data/Statement");

        DomAsserts.assertEquals("statement 1 id",               "stmt1",        dom, "/data/Statement[1]/Sid");
        DomAsserts.assertEquals("statement 1 effect",           "Allow",        dom, "/data/Statement[1]/Effect");
        DomAsserts.assertCount("statement 1 action count",      1,              dom, "/data/Statement[1]/Action");
        DomAsserts.assertEquals("statement 1 action",           "foo",          dom, "/data/Statement[1]/Action");
        DomAsserts.assertCount("statement 1 resource count",    1,              dom, "/data/Statement[1]/Resource");
        DomAsserts.assertEquals("statement 1 resource",         "argle",        dom, "/data/Statement[1]/Resource");

        DomAsserts.assertEquals("statement 2 id",               "stmt2",        dom, "/data/Statement[2]/Sid");
        DomAsserts.assertEquals("statement 2 effect",           "Deny",         dom, "/data/Statement[2]/Effect");
        DomAsserts.assertCount("statement 2 action count",      1,              dom, "/data/Statement[2]/Action");
        DomAsserts.assertEquals("statement 2 action",           "bar",          dom, "/data/Statement[2]/Action");
        DomAsserts.assertCount("statement 2 resource count",    1,              dom, "/data/Statement[2]/Resource");
        DomAsserts.assertEquals("statement 2 resource",         "bargle",       dom, "/data/Statement[2]/Resource");
    }

}