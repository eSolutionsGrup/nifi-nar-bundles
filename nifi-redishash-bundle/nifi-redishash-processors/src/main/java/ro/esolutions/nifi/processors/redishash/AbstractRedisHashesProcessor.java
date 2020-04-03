package ro.esolutions.nifi.processors.redishash;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisConnectionPool;
import org.apache.nifi.redis.util.RedisAction;
import org.springframework.data.redis.connection.RedisConnection;

import java.io.IOException;
import java.util.*;

public abstract class AbstractRedisHashesProcessor extends AbstractSessionFactoryProcessor {

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


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles that are sent successfully to the destination are sent out this relationship.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles that failed to send to the destination are sent out this relationship.")
            .build();

    protected volatile ComponentLog logger;
    private volatile RedisConnectionPool connectionPool;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    protected abstract List<PropertyDescriptor> initPropertyDescriptor();
    protected abstract Set<Relationship> initRelationship();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        logger = getLogger();

        final List<PropertyDescriptor> descriptors = new ArrayList();
        descriptors.add(REDIS_CONNECTION_SERVICE);
        descriptors.add(HASH_PROPERTY);
        descriptors.addAll(initPropertyDescriptor());
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.addAll(initRelationship());
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        connectionPool = context.getProperty(REDIS_CONNECTION_SERVICE).asControllerService(RedisConnectionPool.class);
        final ProcessSession session = sessionFactory.createSession();
        try {
            onTrigger(context, session);
            session.commit();
        } catch (final Throwable t) {
            session.rollback(true);
            throw t;
        }
    }

    public abstract void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;


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
