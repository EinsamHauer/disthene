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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrei Ivanov
 */
@Listener(references = References.Strong)
public class StatsService {
    private static final String SCHEDULER_NAME = "distheneStatsFlusher";

    private Logger logger = Logger.getLogger(StatsService.class);

    private StatsConfiguration statsConfiguration;

    private MBassador<DistheneEvent> bus;
    private Rollup rollup;
    private AtomicLong storeSuccess = new AtomicLong(0);
    private AtomicLong storeError = new AtomicLong(0);
    private final Map<String, StatsRecord> stats = new HashMap<>();

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
        }, statsConfiguration.getInterval(), statsConfiguration.getInterval(), TimeUnit.SECONDS);

    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricReceivedEvent metricReceivedEvent) {
        synchronized (stats) {
            StatsRecord statsRecord = stats.get(metricReceivedEvent.getMetric().getTenant());
            if (statsRecord == null) {
                statsRecord = new StatsRecord();
                stats.put(metricReceivedEvent.getMetric().getTenant(), statsRecord);
            }

            statsRecord.incMetricsReceived();
        }
    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricStoreEvent metricStoreEvent) {
        synchronized (stats) {
            StatsRecord statsRecord = stats.get(metricStoreEvent.getMetric().getTenant());
            if (statsRecord == null) {
                statsRecord = new StatsRecord();
                stats.put(metricStoreEvent.getMetric().getTenant(), statsRecord);
            }

            statsRecord.incMetricsWritten();
        }
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
        long storeSuccess;
        long storeError;

        synchronized (stats) {
            for (Map.Entry<String, StatsRecord> entry : stats.entrySet()) {
                statsToFlush.put(entry.getKey(), new StatsRecord(entry.getValue()));
                entry.getValue().reset();
            }

            storeSuccess = this.storeSuccess.getAndSet(0);
            storeError = this.storeError.getAndSet(0);
        }

        doFlush(statsToFlush, storeSuccess, storeError, DateTime.now(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0).getMillis() / 1000);
    }

    private synchronized void doFlush(Map<String, StatsRecord> stats, long storeSuccess, long storeError, long timestamp) {
        logger.debug("Flushing stats for " + timestamp);

        long totalReceived = 0;
        long totalWritten = 0;

        if (statsConfiguration.isLog()) {
            logger.info("Disthene stats:");
            logger.info("======================================================================================");
            logger.info("\tTenant\tmetrics_received\twrite_count");
            logger.info("======================================================================================");
        }

        for (Map.Entry<String, StatsRecord> entry : stats.entrySet()) {
            String tenant = entry.getKey();
            StatsRecord statsRecord = entry.getValue();

            totalReceived += statsRecord.getMetricsReceived();
            totalWritten += statsRecord.getMetricsWritten();

            Metric metric = new Metric(
                    statsConfiguration.getTenant(),
                    statsConfiguration.getHostname() + ".disthene.tenants." + tenant + ".metrics_received",
                    rollup.getRollup(),
                    rollup.getPeriod(),
                    statsRecord.getMetricsReceived(),
                    timestamp
            );
            bus.post(new MetricStoreEvent(metric)).now();

            metric = new Metric(
                    statsConfiguration.getTenant(),
                    statsConfiguration.getHostname() + ".disthene.tenants." + tenant + ".write_count",
                    rollup.getRollup(),
                    rollup.getPeriod(),
                    statsRecord.getMetricsWritten(),
                    timestamp
            );
            bus.post(new MetricStoreEvent(metric)).now();

            if (statsConfiguration.isLog()) {
                logger.info("\t" + tenant + "\t" + statsRecord.metricsReceived + "\t" + statsRecord.getMetricsWritten());
            }
        }

        Metric metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.metrics_received",
                rollup.getRollup(),
                rollup.getPeriod(),
                totalReceived,
                timestamp
        );
        bus.post(new MetricStoreEvent(metric)).now();

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.write_count",
                rollup.getRollup(),
                rollup.getPeriod(),
                totalWritten,
                timestamp
        );
        bus.post(new MetricStoreEvent(metric)).now();

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.store.success",
                rollup.getRollup(),
                rollup.getPeriod(),
                storeSuccess,
                timestamp
        );
        bus.post(new MetricStoreEvent(metric)).now();

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.store.error",
                rollup.getRollup(),
                rollup.getPeriod(),
                storeError,
                timestamp
        );
        bus.post(new MetricStoreEvent(metric)).now();

        if (statsConfiguration.isLog()) {
            logger.info("\t" + "total" + "\t" + totalReceived + "\t" + totalWritten);
            logger.info("======================================================================================");
            logger.info("\tstore.success:\t" + storeSuccess);
            logger.info("\tstore.error:\t" + storeError);
            logger.info("======================================================================================");
        }
    }

    public synchronized void shutdown() {
        scheduler.shutdown();
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
