package net.iponweb.disthene.config;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrei Ivanov
 */
public class CarbonConfiguration {

    private String bind;
    private int port;
    private int threads;
    private List<Rollup> rollups = new ArrayList<>();
    private Rollup baseRollup;
    private int aggregatorDelay;

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

    // todo: throw exception if list is empty
    public void setRollups(List<Rollup> rollups) {
        baseRollup = rollups.get(0);
        this.rollups = rollups.subList(1, rollups.size());
    }

    public Rollup getBaseRollup() {
        return baseRollup;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    @Override
    public String toString() {
        return "CarbonConfiguration{" +
                "bind='" + bind + '\'' +
                ", port=" + port +
                ", threads=" + threads +
                ", rollups=" + rollups +
                ", baseRollup=" + baseRollup +
                ", aggregatorDelay=" + aggregatorDelay +
                '}';
    }
}
