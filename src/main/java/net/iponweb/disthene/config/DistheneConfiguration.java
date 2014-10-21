package net.iponweb.disthene.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author Andrei Ivanov
 */
@SuppressWarnings("UnusedDeclaration")
@Component
@ConfigurationProperties
public final class DistheneConfiguration {
    private CarbonConfiguration carbon;

    public CarbonConfiguration getCarbon() {
        return carbon;
    }

    public void setCarbon(CarbonConfiguration carbon) {
        this.carbon = carbon;
    }

}
