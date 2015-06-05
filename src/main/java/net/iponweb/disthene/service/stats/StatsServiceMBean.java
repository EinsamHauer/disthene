package net.iponweb.disthene.service.stats;

import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public interface StatsServiceMBean {

    long getStoreSuccess();
    long getStoreError();
    long getMetricsReceived();
    long getWriteCount();

    Map<String, Long> getMetricsReceivedPerTenant();
    Map<String, Long> getWriteCountPerTenant();
}
