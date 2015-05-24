package net.iponweb.disthene.service.stats;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.config.StatsConfiguration;
import net.iponweb.disthene.service.general.GeneralStore;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrei Ivanov
 */
public class Stats {
    private Logger logger = Logger.getLogger(Stats.class);

    private StatsConfiguration statsConfiguration;
    private GeneralStore generalStore;
    private Rollup baseRollup;
    private AtomicLong storeSuccess = new AtomicLong(0);
    private AtomicLong storeError = new AtomicLong(0);
    private ConcurrentHashMap<String, AtomicLong> metricsReceived = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, AtomicLong> metricsWritten = new ConcurrentHashMap<>();


    private final Map<String, StatsRecord> stats = new HashMap<>();
    private ScheduledExecutorService rollupAggregatorScheduler = Executors.newScheduledThreadPool(1);


    public Stats(StatsConfiguration statsConfiguration, Rollup baseRollup) {
        this.statsConfiguration = statsConfiguration;
        this.baseRollup = baseRollup;
    }

    public void setGeneralStore(GeneralStore generalStore) {
        this.generalStore = generalStore;

        rollupAggregatorScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, statsConfiguration.getInterval(), statsConfiguration.getInterval(), TimeUnit.SECONDS);

    }

    public synchronized void incMetricsReceived(String tenant) {
        metricsReceived.putIfAbsent(tenant, new AtomicLong(0));
        metricsReceived.get(tenant).incrementAndGet();
    }

    public synchronized void incMetricsWritten(String tenant) {
        metricsWritten.putIfAbsent(tenant, new AtomicLong(0));
        metricsWritten.get(tenant).incrementAndGet();
    }

    public synchronized void incMetricsReceived(Metric metric) {
/*
        StatsRecord statsRecord = stats.get(metric.getTenant());
        if (statsRecord == null) {
            statsRecord = new StatsRecord();
            stats.put(metric.getTenant(), statsRecord);
        }

        statsRecord.incMetricsReceived();
*/
    }

    public void incStoreSuccess() {
        storeSuccess.addAndGet(1);
    }

    public void incStoreSuccess(int delta) {
        storeSuccess.addAndGet(delta);
    }

    public void incStoreError() {
        storeError.addAndGet(1);
    }

    public void incStoreError(int delta) {
        storeError.addAndGet(delta);
    }

    public synchronized void incMetricsWritten(Metric metric) {
        StatsRecord statsRecord = stats.get(metric.getTenant());
        if (statsRecord == null) {
            statsRecord = new StatsRecord();
            stats.put(metric.getTenant(), statsRecord);
        }

        statsRecord.incMetricsWritten();
    }

    public void flush() {
        long totalReceived = 0;
        long totalWritten = 0;

        if (statsConfiguration.isLog()) {
            logger.info("Disthene stats:");
            logger.info("--------------------------------------------------------------------------------------");
            logger.info("\tTenant\tmetrics_received");
            logger.info("--------------------------------------------------------------------------------------");
        }

        for(ConcurrentMap.Entry<String, AtomicLong> entry : metricsReceived.entrySet()) {
            long value = entry.getValue().getAndSet(0);
            totalReceived += value;
            if (statsConfiguration.isLog()) {
                logger.info("\t" + entry.getKey() + "\t" + value);
            }
        }

        if (statsConfiguration.isLog()) {
            logger.info("--------------------------------------------------------------------------------------");
            logger.info("\tTenant\tmetrics_written");
            logger.info("--------------------------------------------------------------------------------------");
        }

        for(ConcurrentMap.Entry<String, AtomicLong> entry : metricsWritten.entrySet()) {
            long value = entry.getValue().getAndSet(0);
            totalWritten += value;
            if (statsConfiguration.isLog()) {
                logger.info("\t" + entry.getKey() + "\t" + entry.getValue().getAndSet(0));
            }
        }

        if (statsConfiguration.isLog()) {
            logger.info("\t" + "total" + "\t" + totalReceived + "\t" + totalWritten);
            logger.info("--------------------------------------------------------------------------------------");
            logger.info("\tstore.success:\t" + storeSuccess);
            logger.info("\tstore.error:\t" + storeError);
            logger.info("======================================================================================");
        }
    }

    private synchronized void doFlush(Map<String, StatsRecord> stats, long storeSuccess, long storeError, DateTime dt) {
        logger.debug("Flushing stats for " + dt);

        long totalReceived = 0;
        long totalWritten = 0;

        if (statsConfiguration.isLog()) {
            logger.info("Disthene stats:");
            logger.info("======================================================================================");
            logger.info("\tTenant\tmetrics_received\twrite_count");
            logger.info("======================================================================================");
        }

        for(Map.Entry<String, StatsRecord> entry : stats.entrySet()) {
            String tenant = entry.getKey();
            StatsRecord statsRecord = entry.getValue();

            totalReceived += statsRecord.getMetricsReceived();
            totalWritten += statsRecord.getMetricsWritten();

            generalStore.store(new Metric(
                    statsConfiguration.getTenant(),
                    statsConfiguration.getHostname() + ".disthene.tenants." + tenant + ".metrics_received",
                    baseRollup.getRollup(),
                    baseRollup.getPeriod(),
                    statsRecord.getMetricsReceived(),
                    dt
            ));

            generalStore.store(new Metric(
                    statsConfiguration.getTenant(),
                    statsConfiguration.getHostname() + ".disthene.tenants." + tenant + ".write_count",
                    baseRollup.getRollup(),
                    baseRollup.getPeriod(),
                    statsRecord.getMetricsWritten(),
                    dt
            ));

            if (statsConfiguration.isLog()) {
                logger.info("\t" + tenant + "\t" + statsRecord.metricsReceived + "\t" + statsRecord.getMetricsWritten());
            }
        }

        generalStore.store(new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.metrics_received",
                baseRollup.getRollup(),
                baseRollup.getPeriod(),
                totalReceived,
                dt
        ));

        generalStore.store(new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.write_count",
                baseRollup.getRollup(),
                baseRollup.getPeriod(),
                totalWritten,
                dt
        ));

        generalStore.store(new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.store.success",
                baseRollup.getRollup(),
                baseRollup.getPeriod(),
                storeSuccess,
                dt
        ));

        generalStore.store(new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.store.error",
                baseRollup.getRollup(),
                baseRollup.getPeriod(),
                storeError,
                dt
        ));

        if (statsConfiguration.isLog()) {
            logger.info("\t" + "total" + "\t" + totalReceived + "\t" + totalWritten);
            logger.info("======================================================================================");
            logger.info("\tstore.success:\t" + storeSuccess);
            logger.info("\tstore.error:\t" + storeError);
            logger.info("======================================================================================");
        }
    }

    private class StatsRecord {
        private long metricsReceived = 0;
        private long metricsWritten = 0;

        private StatsRecord() {
        }

        private StatsRecord(StatsRecord statsRecord) {
            metricsReceived = statsRecord.metricsReceived;
            metricsWritten = statsRecord.metricsWritten;
        }

        public void reset() {
            metricsReceived = 0;
            metricsWritten = 0;
        }

        public void incMetricsReceived() {
            metricsReceived++;
        }

        public void incMetricsWritten() {
            metricsWritten++;
        }

        public long getMetricsReceived() {
            return metricsReceived;
        }

        public long getMetricsWritten() {
            return metricsWritten;
        }
    }
}
