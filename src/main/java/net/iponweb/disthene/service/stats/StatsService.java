package net.iponweb.disthene.service.stats;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.config.StatsConfiguration;
import net.iponweb.disthene.events.*;
import net.iponweb.disthene.util.NamedThreadFactory;
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
@Listener(references = References.Strong)
public class StatsService implements StatsServiceMBean {
    private static final String SCHEDULER_NAME = "distheneStatsFlusher";

    private Logger logger = Logger.getLogger(StatsService.class);

    private StatsConfiguration statsConfiguration;

    private MBassador<DistheneEvent> bus;
    private Rollup rollup;
    private AtomicLong storeSuccess = new AtomicLong(0);
    private AtomicLong storeError = new AtomicLong(0);
    private ConcurrentMap<String, StatsRecord> stats = new ConcurrentHashMap<>();

    // MBean
    private long lastStoreSuccess = 0;
    private long lastStoreError = 0;
    private long lastMetricsReceived = 0;
    private long lastWriteCount = 0;
    private Map<String, Long> lastMetricsReceivedPerTenant = new HashMap<>();
    private Map<String, Long> lastWriteCountPerTenant = new HashMap<>();


    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory(SCHEDULER_NAME));

    public StatsService(MBassador<DistheneEvent> bus, StatsConfiguration statsConfiguration, Rollup rollup) {
        this.statsConfiguration = statsConfiguration;
        this.bus = bus;
        this.rollup = rollup;
        bus.subscribe(this);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, 60 - ((System.currentTimeMillis() / 1000L) % 60), statsConfiguration.getInterval(), TimeUnit.SECONDS);

    }

    private StatsRecord getStatsRecord(String tenant) {
        StatsRecord statsRecord = stats.get(tenant);
        if (statsRecord == null) {
            StatsRecord newStatsRecord = new StatsRecord();
            statsRecord = stats.putIfAbsent(tenant, newStatsRecord);
            if (statsRecord == null) {
                statsRecord = newStatsRecord;
            }
        }

        return statsRecord;
    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricReceivedEvent metricReceivedEvent) {
        getStatsRecord(metricReceivedEvent.getMetric().getTenant()).incMetricsReceived();
    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricStoreEvent metricStoreEvent) {
        getStatsRecord(metricStoreEvent.getMetric().getTenant()).incMetricsWritten();
    }

    @Handler(rejectSubtypes = false)
    public void handle(StoreSuccessEvent storeSuccessEvent) {
        storeSuccess.addAndGet(storeSuccessEvent.getCount());
    }

    @Handler(rejectSubtypes = false)
    public void handle(StoreErrorEvent storeErrorEvent) {
        storeError.addAndGet(storeErrorEvent.getCount());
    }

    public void flush() {
        Map<String, StatsRecord> statsToFlush = new HashMap<>();
        long storeSuccess = this.storeSuccess.getAndSet(0);
        long storeError = this.storeError.getAndSet(0);

        for (ConcurrentMap.Entry<String, StatsRecord> entry : stats.entrySet()) {
            statsToFlush.put(entry.getKey(), entry.getValue().reset());
        }

        doFlush(statsToFlush, storeSuccess, storeError, DateTime.now(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0).getMillis() / 1000);
    }

    private void doFlush(Map<String, StatsRecord> stats, long storeSuccess, long storeError, long timestamp) {
        logger.debug("Flushing stats for " + timestamp);
        lastMetricsReceivedPerTenant.clear();
        lastWriteCountPerTenant.clear();

        long totalReceived = 0;
        long totalWritten = 0;

        if (statsConfiguration.isLog()) {
            logger.info("Disthene stats:");
            logger.info("=========================================================================");
            logger.info("Tenant\t\tmetrics_received\t\twrite_count");
            logger.info("=========================================================================");
        }

        for (Map.Entry<String, StatsRecord> entry : stats.entrySet()) {
            String tenant = entry.getKey();
            StatsRecord statsRecord = entry.getValue();

            totalReceived += statsRecord.getMetricsReceived();
            totalWritten += statsRecord.getMetricsWritten();

            Metric metric = new Metric(
                    statsConfiguration.getTenant(),
                    statsConfiguration.getPath() + statsConfiguration.getHostname() + ".disthene.tenants." + tenant + ".metrics_received",
                    rollup.getRollup(),
                    rollup.getPeriod(),
                    statsRecord.getMetricsReceived(),
                    timestamp
            );
            bus.post(new MetricStoreEvent(metric)).now();
            lastMetricsReceivedPerTenant.put(tenant, statsRecord.getMetricsReceived());

            metric = new Metric(
                    statsConfiguration.getTenant(),
                    statsConfiguration.getPath() + statsConfiguration.getHostname() + ".disthene.tenants." + tenant + ".write_count",
                    rollup.getRollup(),
                    rollup.getPeriod(),
                    statsRecord.getMetricsWritten(),
                    timestamp
            );
            bus.post(new MetricStoreEvent(metric)).now();
            lastMetricsReceivedPerTenant.put(tenant, statsRecord.getMetricsWritten());

            if (statsConfiguration.isLog()) {
                logger.info(tenant + "\t\t" + statsRecord.metricsReceived + "\t\t" + statsRecord.getMetricsWritten());
            }
        }

        Metric metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getPath() + statsConfiguration.getHostname() + ".disthene.metrics_received",
                rollup.getRollup(),
                rollup.getPeriod(),
                totalReceived,
                timestamp
        );
        bus.post(new MetricStoreEvent(metric)).now();
        lastMetricsReceived = totalReceived;

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getPath() + statsConfiguration.getHostname() + ".disthene.write_count",
                rollup.getRollup(),
                rollup.getPeriod(),
                totalWritten,
                timestamp
        );
        bus.post(new MetricStoreEvent(metric)).now();
        lastWriteCount = totalWritten;

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getPath() + statsConfiguration.getHostname() + ".disthene.store.success",
                rollup.getRollup(),
                rollup.getPeriod(),
                storeSuccess,
                timestamp
        );
        bus.post(new MetricStoreEvent(metric)).now();
        lastStoreSuccess = storeSuccess;

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getPath() + statsConfiguration.getHostname() + ".disthene.store.error",
                rollup.getRollup(),
                rollup.getPeriod(),
                storeError,
                timestamp
        );
        bus.post(new MetricStoreEvent(metric)).now();
        lastStoreError = storeError;

        if (statsConfiguration.isLog()) {
            logger.info("total\t\t" + totalReceived + "\t\t" + totalWritten);
            logger.info("=========================================================================");
            logger.info("store.success:\t\t" + storeSuccess);
            logger.info("store.error:\t\t" + storeError);
            logger.info("=========================================================================");
        }
    }

    public synchronized void shutdown() {
        scheduler.shutdown();
    }

    // MBean


    @Override
    public long getStoreSuccess() {
        return lastStoreSuccess;
    }

    @Override
    public long getStoreError() {
        return lastStoreError;
    }

    @Override
    public long getMetricsReceived() {
        return lastMetricsReceived;
    }

    @Override
    public long getWriteCount() {
        return lastWriteCount;
    }

    @Override
    public Map<String, Long> getMetricsReceivedPerTenant() {
        return lastMetricsReceivedPerTenant;
    }

    @Override
    public Map<String, Long> getWriteCountPerTenant() {
        return lastWriteCountPerTenant;
    }

    private class StatsRecord {
        private AtomicLong metricsReceived = new AtomicLong(0);
        private AtomicLong metricsWritten = new AtomicLong(0);

        private StatsRecord() {
        }

        public StatsRecord(long metricsReceived, long metricsWritten) {
            this.metricsReceived = new AtomicLong(metricsReceived);
            this.metricsWritten = new AtomicLong(metricsWritten);
        }

        /**
         * Resets the stats to zeroes and returns a snapshot of the record
         * @return snapshot of the record
         */
        public StatsRecord reset() {
            return new StatsRecord(metricsReceived.getAndSet(0), metricsWritten.getAndSet(0));
        }

        public void incMetricsReceived() {
            metricsReceived.addAndGet(1);
        }

        public void incMetricsWritten() {
            metricsWritten.addAndGet(1);
        }

        public long getMetricsReceived() {
            return metricsReceived.get();
        }

        public long getMetricsWritten() {
            return metricsWritten.get();
        }
    }
}
