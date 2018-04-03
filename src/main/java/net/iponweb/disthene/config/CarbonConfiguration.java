package net.iponweb.disthene.config;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrei Ivanov
 */
public class CarbonConfiguration {

    private String bind;
    private int port;
    private List<Rollup> rollups = new ArrayList<>();
    private Rollup baseRollup;
    private int aggregatorDelay;
    private List<String> authorizedTenants = new ArrayList<>();
    private boolean allowAll = true;

    public String getBind() {
        return bind;
    }

    public void setBind(String bind) {
        this.bind = bind;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getAggregatorDelay() {
        return aggregatorDelay;
    }

    public void setAggregatorDelay(int aggregatorDelay) {
        this.aggregatorDelay = aggregatorDelay;
    }

    public List<Rollup> getRollups() {
        return rollups;
    }

    public void setRollups(List<Rollup> rollups) {
        baseRollup = rollups.get(0);
        this.rollups = rollups.subList(1, rollups.size());
    }

    public Rollup getBaseRollup() {
        return baseRollup;
    }

    public List<String> getAuthorizedTenants() {
        return authorizedTenants;
    }

    public void setAuthorizedTenants(List<String> authorizedTenants) {
        this.authorizedTenants = authorizedTenants;
    }

    public boolean isAllowAll() {
        return allowAll;
    }

    public void setAllowAll(boolean allowAll) {
        this.allowAll = allowAll;
    }

    @Override
    public String toString() {
        return "CarbonConfiguration{" +
                "bind='" + bind + '\'' +
                ", port=" + port +
                ", rollups=" + rollups +
                ", baseRollup=" + baseRollup +
                ", aggregatorDelay=" + aggregatorDelay +
                ", authorizedTenants=" + authorizedTenants +
                ", allowAll=" + allowAll +
                '}';
    }
}
