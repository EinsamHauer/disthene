package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.config.DriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;

public enum CustomDriverOption implements DriverOption {
    CUSTOM_NUMBER_OF_RETRIES("custom.retry-policy.max-retries"),
    ;

    private final String path;

    CustomDriverOption(String path) {
        this.path = path;
    }

    @NonNull
    @Override
    public String getPath() {
        return path;
    }
}