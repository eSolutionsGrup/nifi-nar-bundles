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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.util.RedisAction;
import org.springframework.data.redis.connection.RedisConnection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@EventDriven
@SupportsBatching
@Tags({"redis", "hash", "expire", "distributed"})
@CapabilityDescription("Set TTL for redis hash key redis.")
@SeeAlso(classNames = {"org.apache.nifi.redis.service.RedisConnectionPool"})
public class RedisExpireProcessor extends AbstractProcessor {

    static final String HASH_KEY_ATTR = "hash.key";

    public static final PropertyDescriptor REDIS_CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("Redis Connection Pool")
            .description("The Controller Service that is used to get the cached values.")
            .required(true)
            .identifiesControllerService(RedisConnectionPool.class)
            .build();

    public static final PropertyDescriptor HASH_PROPERTY = new PropertyDescriptor
            .Builder().name("Hash Key")
            .description("A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated against a FlowFile in order to determine the hash key")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TTL_PROPERTY = new PropertyDescriptor
            .Builder().name("TTL")
            .description("A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated against a FlowFile in order to determine the hash key")
            .required(true)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are sent out this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are sent out this relationship.")
            .build();

    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
            .name("not.found")
            .description("FlowFile that fail because the hash or field do not exist will be routed to this relationship.")
            .build();

    protected volatile ComponentLog logger;
    private volatile RedisConnectionPool connectionPool;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;


    @Override
    protected void init(ProcessorInitializationContext context) {
        logger = getLogger();

        final List<PropertyDescriptor> descriptors = new ArrayList();
        descriptors.add(REDIS_CONNECTION_SERVICE);
        descriptors.add(HASH_PROPERTY);
        descriptors.add(TTL_PROPERTY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_NOT_FOUND);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        connectionPool = context.getProperty(REDIS_CONNECTION_SERVICE).asControllerService(RedisConnectionPool.class);

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String hashKey = context.getProperty(HASH_PROPERTY).evaluateAttributeExpressions(flowFile).getValue();
        Long ttl = Long.parseLong(context.getProperty(TTL_PROPERTY).evaluateAttributeExpressions(flowFile).getValue());

        flowFile = session.putAttribute(flowFile, HASH_KEY_ATTR, hashKey);
        connectionPool = context.getProperty(REDIS_CONNECTION_SERVICE).asControllerService(RedisConnectionPool.class);

        try {
            Boolean success = withConnection(redisConnection ->
                    redisConnection.expire(
                            hashKey.getBytes(StandardCharsets.UTF_8),
                            ttl));

            if(success) {
                session.transfer(flowFile, REL_SUCCESS);
            } else {
                session.transfer(flowFile, REL_NOT_FOUND);
            }

        } catch (IOException e) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            logger.error("Unable to communicate with redis when processing {} due to {}", new Object[]{flowFile, e});
        }
    }

    protected  <T> T withConnection(final RedisAction<T> action) throws IOException {
        RedisConnection redisConnection = null;
        try {
            redisConnection = connectionPool.getConnection();
            return action.execute(redisConnection);
        } finally {
            if (redisConnection != null) {
                try {
                    redisConnection.close();
                } catch (Exception e) {
                    getLogger().warn("Error closing connection: " + e.getMessage(), e);
                }
            }
        }
    }
}
