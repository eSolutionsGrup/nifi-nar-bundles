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

import org.apache.commons.lang3.StringUtils;
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
import org.apache.nifi.redis.util.RedisAction;

import java.io.IOException;
import java.util.*;

@EventDriven
@SupportsBatching
@Tags({"redis", "hash", "set", "distributed"})
@CapabilityDescription("Get the content of a FlowFile and put it to redis, using a hash key and field computed from FlowFile attributes")
@SeeAlso(classNames = {"org.apache.nifi.redis.service.RedisConnectionPool"})
public class RedisHGetProcessor extends AbstractRedisHashesProcessor {

    public static final PropertyDescriptor FIELD_PROPERTY = new PropertyDescriptor
            .Builder().name("Field Key")
            .description("Specifies the field in the hash stored. If not specified, returns all the values of all the fields from the hash stored at key.")
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();


    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("Any FlowFile for which we receive a 'Empty' message from the remote server will be transferred to this Relationship.")
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
        if (flowFile == null) {
            return;
        }

        String hashKey = context.getProperty(HASH_PROPERTY).evaluateAttributeExpressions(flowFile).getValue();
        String field = context.getProperty(FIELD_PROPERTY).evaluateAttributeExpressions(flowFile).getValue();

        try {
            Map<String, byte[]> cacheValues = withConnection(actionHGet(hashKey, field));
            if (cacheValues.isEmpty()) {
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {
                for (Map.Entry<String, byte[]> cacheValueEntry : cacheValues.entrySet()) {
                    final byte[] cacheValue = cacheValueEntry.getValue();
                    flowFile = session.write(flowFile, out -> out.write(cacheValue));
                }
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with redis when processing {} due to {}", new Object[]{flowFile, e});
        }
    }

    private RedisAction<Map<String, byte[]>> actionHGet(String hashKey, String field) {
        return redisConnection -> {
            Map<String, byte[]> response = new HashMap<>();

            if (StringUtils.isBlank(field)) {
                Objects.requireNonNull(redisConnection.hGetAll(hashKey.getBytes()))
                        .forEach((key, value) -> {
                            if (null != value) {
                                response.put(new String(key), value);
                            }
                        });
            } else {
                byte[] value = redisConnection.hGet(hashKey.getBytes(), field.getBytes());
                if (null != value) {
                    response.put(field, value);
                }
            }

            return response;
        };
    }
}
