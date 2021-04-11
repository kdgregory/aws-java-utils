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

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;


/**
 *  Provides a fluent approach to building AWS policies at runtime, minimizing
 *  the chances for typos.
 */
public class PolicyBuilder
{
    private static JsonFactory jf = new JsonFactory();

    private String version = "2012-10-17";
    private String id      = UUID.randomUUID().toString();
    private List<Statement> statements = new ArrayList<Statement>();

//----------------------------------------------------------------------------
//  Top level
//----------------------------------------------------------------------------

    /**
     *  Assigns a specific ID to this policy. This is optional: if not used,
     *  the policy will be assigned a random UUID.
     */
    public PolicyBuilder id(String value)
    {
        id = value;
        return this;
    }


    /**
     *  Adds one or more statements to this policy. This method may be called
     *  multiple times; the statements are added in the order seen.
     */
    public PolicyBuilder statements(Statement... values)
    {
        for (Statement value : values)
        {
            statements.add(value);
        }
        return this;
    }


    /**
     *  Creates the JSON representation of this policy.
     */
    public String build()
    {
        StringWriter sw = new StringWriter(8192);
        try (JsonGenerator out = jf.createGenerator(sw))
        {
            out.writeStartObject();
            out.writeStringField("Version", version);
            out.writeStringField("Id", id);
            out.writeArrayFieldStart("Statement");
            for (Statement stmt : statements)
            {
                stmt.append(out);
            }
            out.writeEndArray();
            out.writeEndObject();
            out.close();
            return sw.toString();
        }
        catch (IllegalArgumentException ex)
        {
            throw ex;
        }
        catch (Exception ex)
        {
            throw new RuntimeException("unexpected exception producing JSON output", ex);
        }
    }

//----------------------------------------------------------------------------
//  Statements
//----------------------------------------------------------------------------

    public static class Statement
    {
        private String sid;
        private String effect;
        private Principal principal;
        private Set<String> actions = new TreeSet<String>();
        private Set<String> notActions = new TreeSet<String>();
        private Set<String> resources = new TreeSet<String>();
        private Set<String> notResources = new TreeSet<String>();
        private LinkedHashMap<String,Condition> conditions = new LinkedHashMap<String,Condition>();


        /**
         *  Sets the statement ID.
         */
        public Statement sid(String value)
        {
            sid = value;
            return this;
        }


        /**
         *  Allow access to specified resources.
         */
        public Statement allow()
        {
            effect = "Allow";
            return this;
        }


        /**
         *  Deny access to specified resources.
         */
        public Statement deny()
        {
            effect = "Deny";
            return this;
        }


        /**
         *  Adds a {@link #Principal} or {@link #NotPrincipal} to this statement.
         *  This is optional.
         */
        public Statement principal(Principal value)
        {
            principal = value;
            return this;
        }


        /**
         *  Specifies one or more actions conveyed by this statement. This method
         *  may be called multiple times; the actions are combined. No attempt is
         *  made to validate the actions.
         *  <p>
         *  A single statement can specify either actions or not-actions. Attempting
         *  to configure both will result in an exception when building the policy.
         */
        public Statement actions(String... values)
        {
            actions.addAll(Arrays.asList(values));
            return this;
        }


        /**
         *  Specifies one or more actions excluded by this statement. This method
         *  may be called multiple times; the actions are combined. No attempt is
         *  made to validate the actions.
         *  <p>
         *  A single statement can specify either actions or not-actions. Attempting
         *  to configure both will result in an exception when building the policy.
         */
        public Statement notActions(String... values)
        {
            notActions.addAll(Arrays.asList(values));
            return this;
        }


        /**
         *  Specifies one or more resources to which this statement applies. This
         *  method may be called multiple times; the resources are combined. No
         *  attempt is made to validate the resources.
         *  <p>
         *  A single statement can specify either resources or not-resources.
         *  Attempting to configure both will result in an exception when building
         *  the policy.
         */
        public Statement resources(String... values)
        {
            resources.addAll(Arrays.asList(values));
            return this;
        }


        /**
         *  Specifies one or more resources excluded by this statement. This
         *  method may be called multiple times; the resources are combined.
         *  No attempt is made to validate the resources.
         *  <p>
         *  A single statement can specify either resources or not-resources.
         *  Attempting to configure both will result in an exception when building
         *  the policy.
         */
        public Statement notResources(String... values)
        {
            notResources.addAll(Arrays.asList(values));
            return this;
        }


        /**
         *  Specifies one or more conditions that apply to this statement. This
         *  method may be called multiple times; the resources are combined. May
         *  not specify multiple conditions with the same key.
         */
        public Statement conditions(Condition... values)
        {
            for (Condition value : values)
            {
                String type = value.getType();
                if (conditions.containsKey(type))
                    throw new IllegalArgumentException("specified multiple conditions of same type: " + type);
                conditions.put(type, value);
            }
            return this;
        }


        /**
         *  Internal method to append this statement to an in-process build.
         */
        protected void append(JsonGenerator out)
        throws IOException
        {
            if (effect == null)
                throw new IllegalArgumentException("statement must specify effect");
            if (actions.isEmpty() && notActions.isEmpty())
                throw new IllegalArgumentException("statement must specify actions or not-actions");
            if (! actions.isEmpty() && ! notActions.isEmpty())
                throw new IllegalArgumentException("statement must specify either actions or not-actions, not both");
            if (resources.isEmpty() && notResources.isEmpty())
                throw new IllegalArgumentException("statement must specify resources or not-resources");
            if (! resources.isEmpty() && ! notResources.isEmpty())
                throw new IllegalArgumentException("statement must specify either resources or not-resources, not both");

            out.writeStartObject();

            if (sid != null)
                out.writeStringField("Sid", sid);

            out.writeStringField("Effect", effect);

            if (principal != null)
            {
                principal.appendHeader(out);
                principal.appendBody(out);
            }

            if (! actions.isEmpty())
                appendListField(out, "Action", actions);
            else
                appendListField(out, "NotAction", notActions);

            if (! resources.isEmpty())
                appendListField(out, "Resource", resources);
            else
                appendListField(out, "NotResource", notResources);

            if (! conditions.isEmpty())
            {
                out.writeObjectFieldStart("Condition");
                for (Condition value : conditions.values())
                {
                    value.append(out);
                }
                out.writeEndObject();
            }
            out.writeEndObject();
        }
    }

//----------------------------------------------------------------------------
//  Principal
//----------------------------------------------------------------------------

    /**
     *  Represents a Principal. A principal may allow unrestricted access, or
     *  may be limited to some combination of AWS accounts/users, federated
     *  identities, and AWS services. If you specify "all", then any other
     *  configuration is ignored.
     */
    public static class Principal
    {
        private boolean all;
        private Set<String> awsIdentities = new TreeSet<String>();
        private Set<String> awsServices = new TreeSet<String>();
        private Set<String> federatedIdentities = new TreeSet<String>();


        /**
         *  Configures this principal to allow unrestricted access.
         */
        public Principal all()
        {
            this.all = true;
            return this;
        }


        /**
         *  Adds one or more AWS identities to this principal. You can call this
         *  multiple times; entries will be aggregated, and duplicates removed.
         */
        public Principal awsIdentities(String... values)
        {
            awsIdentities.addAll(Arrays.asList(values));
            return this;
        }


        /**
         *  Adds one or more federated identities to this principal. You can call this
         *  multiple times; entries will be aggregated, and duplicates removed.
         */
        public Principal federatedIdentities(String... values)
        {
            federatedIdentities.addAll(Arrays.asList(values));
            return this;
        }


        /**
         *  Adds one or more AWS services to this principal. You can call this
         *  multiple times; entries will be aggregated, and duplicates removed.
         */
        public Principal awsServices(String... values)
        {
            awsServices.addAll(Arrays.asList(values));
            return this;
        }


        /**
         *  Internal method to append the field header of this principal to an
         *  in-process build.
         */
        protected void appendHeader(JsonGenerator out)
        throws IOException
        {
            out.writeFieldName("Principal");
        }


        /**
         *  Internal method to append the body of this principal to an in-process build.
         */
        protected void appendBody(JsonGenerator out)
        throws IOException
        {
            if (all)
            {
                out.writeString("*");
            }
            else
            {
                out.writeStartObject();
                appendOptEntityList(out, "AWS", awsIdentities);
                appendOptEntityList(out, "Federated", federatedIdentities);
                appendOptEntityList(out, "Service", awsServices);
                out.writeEndObject();
            }
        }


        /**
         *  Internal method to append an entity list.
         */
        private void appendOptEntityList(JsonGenerator out, String type, Set<String> list)
        throws IOException
        {
            if (list.isEmpty())
                return;

            out.writeArrayFieldStart(type);
            for (String value : list)
                out.writeString(value);
            out.writeEndArray();
        }
    }


    /**
     *  Represents a NotPrincipal. This has identical behavior to Principal, but is
     *  emitted differently in the statement.
     */
    public static class NotPrincipal extends Principal
    {
        @Override
        protected void appendHeader(JsonGenerator out)
        throws IOException
        {
            out.writeFieldName("NotPrincipal");
        }
    }

//----------------------------------------------------------------------------
//  Conditions
//----------------------------------------------------------------------------

    public static class Condition
    {
        private String type;
        private String key;
        private List<String> values;


        /**
         *  Creates a single condition entry.
         */
        public Condition(String type, String key, String ...values)
        {
            this.type = type;
            this.key = key;
            this.values = Arrays.asList(values);
        }


        /**
         *  Returns the type for this condition. Used to verify that multiple
         *  conditions with the same type are not being added to the statement.
         */
        public String getType()
        {
            return type;
        }


        /**
         *  Internal method to append this condition to an in-process build.
         */
        protected void append(JsonGenerator out)
        throws IOException
        {
            out.writeFieldName(type);
            out.writeStartObject();
            appendListField(out, key, values);
            out.writeEndObject();
        }
    }

//----------------------------------------------------------------------------
//  Internal utility methods
//----------------------------------------------------------------------------

    /**
     *  Writes a field whose value may be an array or a single value.
     */
    private static void appendListField(JsonGenerator out, String key, Collection<String> values)
    throws IOException
    {
        out.writeFieldName(key);
        if (values.size() == 1)
        {
            out.writeString(values.iterator().next());
        }
        else
        {
            out.writeStartArray();
            for (String value : values)
            {
                out.writeString(value);
            }
            out.writeEndArray();
        }
    }
}
