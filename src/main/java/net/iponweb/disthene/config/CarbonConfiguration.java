package net.iponweb.disthene.config;

import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrei Ivanov
 */
@SuppressWarnings("UnusedDeclaration")
@Component
public class CarbonConfiguration {

    private InetAddress bind;
    private int port;
    private List<Rollup> rollups = new ArrayList<>();
    private Rollup baseRollup;

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

    public void setRollups(String rollups) {
        String[] ss = rollups.split(";");
        baseRollup = new Rollup(ss[0]);

        for (int i = 1; i < ss.length; i++) {
            this.rollups.add(new Rollup(ss[i]));
        }
    }

    public List<Rollup> getRollups() {
        return rollups;
    }

    public Rollup getBaseRollup() {
        return baseRollup;
    }

}
