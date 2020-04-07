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

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.util.RedisAction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@EventDriven
@SupportsBatching
@Tags({"redis", "hash", "get", "fetch", "read", "distributed"})
@CapabilityDescription("Retriever all fields or a field and values of the hash stored at key as JSON document.")
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

    private ObjectMapper objectMapper = new ObjectMapper();

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

        flowFile = session.putAttribute(flowFile, HASH_KEY_ATTR, hashKey);
        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");

        try {
            List<HashesResponse> cacheValues = withConnection(actionHGet(hashKey, field));
            if (cacheValues.isEmpty()) {
                session.transfer(flowFile, REL_NOT_FOUND);
            } else {

                flowFile = session.write(flowFile, out ->
                        out.write(objectMapper.writeValueAsString(cacheValues).getBytes(StandardCharsets.UTF_8)));
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with redis when processing {} due to {}", new Object[]{flowFile, e});
        }
    }

    private RedisAction<List<HashesResponse>> actionHGet(String hashKey, String field) {
        return redisConnection -> {
            final byte[] hashByte = hashKey.getBytes(StandardCharsets.UTF_8);

            if (StringUtils.isBlank(field)) {
                return Optional.ofNullable(redisConnection.hGetAll(hashByte))
                        .map(r ->
                            r.entrySet().stream()
                            .filter(e -> e.getValue() != null)
                            .map(entry -> toRedisResponse(new String(entry.getKey()), entry.getValue()))
                            .collect(Collectors.toList())
                        ).orElseGet(ArrayList::new);
            } else {
                byte[] fieldByte = field.getBytes(StandardCharsets.UTF_8);

                return Optional.ofNullable(redisConnection.hGet(hashByte, fieldByte))
                    .map(value -> Collections.singletonList(toRedisResponse(field, value)))
                    .orElseGet(ArrayList::new);
            }
        };
    }

    private HashesResponse toRedisResponse(String field, byte[] value) {
        try {
            return new HashesResponse(field, objectMapper.readTree(value));
        } catch (IOException e) {
            return new HashesResponse(field, new String(value));
        }
    }

    private class HashesResponse {
        private final String field;
        private final Object value;

        private HashesResponse(String field, Object value) {
            this.field = field;
            this.value = value;
        }

        public String getField() {
            return field;
        }

        public Object getValue() {
            return value;
        }
    }
}
