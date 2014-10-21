package net.iponweb.disthene.config;

import org.springframework.stereotype.Component;

import java.net.InetAddress;

/**
 * @author Andrei Ivanov
 */
@SuppressWarnings("UnusedDeclaration")
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
