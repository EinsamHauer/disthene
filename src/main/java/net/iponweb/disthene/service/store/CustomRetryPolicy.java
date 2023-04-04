package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.retry.DefaultRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomRetryPolicy extends DefaultRetryPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryPolicy.class);

    private final String logPrefix;
    private final int maxRetries;

    private static final int DEFAULT_MAX_RETRIES = 10;

    public CustomRetryPolicy(DriverContext context, String profileName) {
        super(context, profileName);
        this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;

        maxRetries = context != null ?  context.getConfig().getProfile(profileName).getInt(CustomDriverOption.CUSTOM_NUMBER_OF_RETRIES, DEFAULT_MAX_RETRIES) : DEFAULT_MAX_RETRIES;
    }

    @SuppressWarnings("deprecation")
    @Override
    public RetryDecision onWriteTimeout(@NonNull Request request, @NonNull ConsistencyLevel cl, @NonNull WriteType writeType, int blockFor, int received, int retryCount) {
        RetryDecision decision =
                (retryCount <= maxRetries)
                        ? RetryDecision.RETRY_NEXT
                        : RetryDecision.RETHROW;

        if (decision == RetryDecision.RETRY_NEXT && LOG.isTraceEnabled()) {
            LOG.info(
                    RETRYING_ON_WRITE_TIMEOUT, logPrefix, cl, writeType, blockFor, received, retryCount);
        }
        return decision;
    }
}