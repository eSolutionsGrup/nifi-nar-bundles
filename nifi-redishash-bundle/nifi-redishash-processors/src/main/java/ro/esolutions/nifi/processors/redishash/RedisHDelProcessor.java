/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ro.esolutions.nifi.processors.redishash;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EventDriven
@SupportsBatching
@Tags({"redis", "hash", "delete", "distributed"})
@CapabilityDescription("Remove from redis, using a hash key and field computed from FlowFile attributes")
@SeeAlso(classNames = {"org.apache.nifi.redis.service.RedisConnectionPool"})
public class RedisHDelProcessor extends AbstractRedisHashesProcessor {

    public static final PropertyDescriptor FIELD_PROPERTY = new PropertyDescriptor
            .Builder().name("Field Key")
            .description("Specifies the field in the hash stored at key. If specified fields that do not exist, it will be transferred to 'Not Found' relationship.")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("FlowFile that fail to delete because the hash or field do not exist will be routed to this relationship.")
            .build();


    @Override
    protected List<PropertyDescriptor> initPropertyDescriptor() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FIELD_PROPERTY);
        return descriptors;
    }

    @Override
    protected Set<Relationship> initRelationship() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_NOT_FOUND);
        return relationships;
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        String hashKey = context.getProperty(HASH_PROPERTY).evaluateAttributeExpressions(flowFile).getValue();
        String field = context.getProperty(FIELD_PROPERTY).evaluateAttributeExpressions(flowFile).getValue();

        flowFile = session.putAttribute(flowFile, HASH_KEY_ATTR, hashKey);

        try {
            Long result = withConnection(redisConnection ->
                    redisConnection.hDel(
                            hashKey.getBytes(StandardCharsets.UTF_8),
                            field.getBytes(StandardCharsets.UTF_8)));

            if(result > 0L) {
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_NOT_FOUND);
            }
        } catch (IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with redis when processing {} due to {}", new Object[] {flowFile, e});
        }
    }

}
