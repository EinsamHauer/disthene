package net.iponweb.disthene.config;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Andrei Ivanov
 */
public class StatsConfiguration {

    private int interval;
    private String tenant;
    private String hostname;
    private String pathPrefix;
    private boolean log;

    public StatsConfiguration() {
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "unknown";
        }
        pathPrefix = "";
    }

    public int getInterval() {
        return interval;
    }


    public void setInterval(int interval) {
        this.interval = interval;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getPathPrefix() {
        return pathPrefix;
    }

    public void setPathPrefix(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    public boolean isLog() {
        return log;
    }

    public void setLog(boolean log) {
        this.log = log;
    }

    @Override
    public String toString() {
        return "StatsConfiguration{" +
                "interval=" + interval +
                ", tenant='" + tenant + '\'' +
                ", hostname='" + hostname + '\'' +
                ", pathPrefix='" + pathPrefix + '\'' +
                ", log=" + log +
                '}';
    }
}
