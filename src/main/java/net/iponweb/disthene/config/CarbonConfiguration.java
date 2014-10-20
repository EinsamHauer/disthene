package net.iponweb.disthene.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.net.InetAddress;

/**
 * @author Andrei Ivanov
 */
@Component
public class CarbonConfiguration {

    private InetAddress bind;
    private int port;

    public InetAddress getBind() {
        return bind;
    }

    public void setBind(InetAddress bind) {
        this.bind = bind;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }
}
