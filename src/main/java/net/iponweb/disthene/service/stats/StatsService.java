package net.iponweb.disthene.service.stats;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bus.DistheneBus;
import net.iponweb.disthene.bus.DistheneEventListener;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.config.StatsConfiguration;
import net.iponweb.disthene.events.*;
import net.iponweb.disthene.util.NameThreadFactory;
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
public class StatsService implements DistheneEventListener{
    private static final String SCHEDULER_NAME = "distheneStatsFlusher";

    private Logger logger = Logger.getLogger(StatsService.class);

    private StatsConfiguration statsConfiguration;

    private DistheneBus bus;
    private Rollup rollup;
    private AtomicLong storeSuccess = new AtomicLong(0);
    private AtomicLong storeError = new AtomicLong(0);
    private final Map<String, StatsRecord> stats = new HashMap<>();

    public StatsService(DistheneBus bus, StatsConfiguration statsConfiguration, Rollup rollup) {
        this.statsConfiguration = statsConfiguration;
        this.bus = bus;
        this.rollup = rollup;
        bus.subscribe(MetricReceivedEvent.class, this);
        bus.subscribe(MetricStoreEvent.class, this);
        bus.subscribe(StoreSuccessEvent.class, this);
        bus.subscribe(StoreErrorEvent.class, this);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NameThreadFactory(SCHEDULER_NAME));
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, statsConfiguration.getInterval(), statsConfiguration.getInterval(), TimeUnit.SECONDS);

    }

    @Override
    public void handle(DistheneEvent event) {
        if (event instanceof MetricReceivedEvent) {
            synchronized (stats) {
                StatsRecord statsRecord = stats.get(((MetricReceivedEvent) event).getMetric().getTenant());
                if (statsRecord == null) {
                    statsRecord = new StatsRecord();
                    stats.put(((MetricReceivedEvent) event).getMetric().getTenant(), statsRecord);
                }

                statsRecord.incMetricsReceived();
            }
        } else if (event instanceof MetricStoreEvent) {
            synchronized (stats) {
                StatsRecord statsRecord = stats.get(((MetricStoreEvent) event).getMetric().getTenant());
                if (statsRecord == null) {
                    statsRecord = new StatsRecord();
                    stats.put(((MetricStoreEvent) event).getMetric().getTenant(), statsRecord);
                }

                statsRecord.incMetricsWritten();
            }
        } else if (event instanceof StoreSuccessEvent) {
            storeSuccess.addAndGet(((StoreSuccessEvent) event).getCount());
        } else if (event instanceof StoreErrorEvent) {
            storeError.addAndGet(((StoreErrorEvent) event).getCount());
        }
    }

    public void flush() {
        Map<String, StatsRecord> statsToFlush = new HashMap<>();
        long storeSuccess;
        long storeError;

        synchronized (stats) {
            for(Map.Entry<String, StatsRecord> entry : stats.entrySet()) {
                statsToFlush.put(entry.getKey(), new StatsRecord(entry.getValue()));
                entry.getValue().reset();
            }

            storeSuccess = this.storeSuccess.getAndSet(0);
            storeError = this.storeError.getAndSet(0);
        }

        doFlush(statsToFlush, storeSuccess, storeError, DateTime.now(DateTimeZone.UTC).withSecondOfMinute(0).withMillisOfSecond(0));
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

            Metric metric = new Metric(
                    statsConfiguration.getTenant(),
                    statsConfiguration.getHostname() + ".disthene.tenants." + tenant + ".metrics_received",
                    rollup.getRollup(),
                    rollup.getPeriod(),
                    statsRecord.getMetricsReceived(),
                    dt
            );
            bus.post(new MetricStoreEvent(metric));

            metric = new Metric(
                    statsConfiguration.getTenant(),
                    statsConfiguration.getHostname() + ".disthene.tenants." + tenant + ".write_count",
                    rollup.getRollup(),
                    rollup.getPeriod(),
                    statsRecord.getMetricsWritten(),
                    dt
            );
            bus.post(new MetricStoreEvent(metric));

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
                dt
        );
        bus.post(new MetricStoreEvent(metric));

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.write_count",
                rollup.getRollup(),
                rollup.getPeriod(),
                totalWritten,
                dt
        );
        bus.post(new MetricStoreEvent(metric));

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.store.success",
                rollup.getRollup(),
                rollup.getPeriod(),
                storeSuccess,
                dt
        );
        bus.post(new MetricStoreEvent(metric));

        metric = new Metric(
                statsConfiguration.getTenant(),
                statsConfiguration.getHostname() + ".disthene.store.error",
                rollup.getRollup(),
                rollup.getPeriod(),
                storeError,
                dt
        );
        bus.post(new MetricStoreEvent(metric));

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
