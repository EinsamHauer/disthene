package net.iponweb.disthene.service.aggregate;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricKey;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricStoreEvent;
import net.iponweb.disthene.events.StoreErrorEvent;
import net.iponweb.disthene.events.StoreSuccessEvent;
import net.iponweb.disthene.util.NamedThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * @author Andrei Ivanov
 */
@Listener(references = References.Strong)
public class RollupService {
    private static final String SCHEDULER_NAME = "distheneRollupAggregatorFlusherScheduler";
    private static final String FLUSHER_NAME = "distheneRollupAggregatorFlusher";
    private static final int RATE = 60;
    private volatile boolean shuttingDown = false;

    private static final Logger logger = LogManager.getLogger(RollupService.class);

    private final MBassador<DistheneEvent> bus;
    private final DistheneConfiguration distheneConfiguration;
    private Rollup maxRollup;
    private final List<Rollup> rollups;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory(SCHEDULER_NAME));
    private final ExecutorService flusher = Executors.newCachedThreadPool(new NamedThreadFactory(FLUSHER_NAME));

    private final ConcurrentNavigableMap<Long, ConcurrentMap<MetricKey, AverageRecord>> accumulator = new ConcurrentSkipListMap<>();

    public RollupService(MBassador<DistheneEvent> bus, DistheneConfiguration distheneConfiguration, List<Rollup> rollups) {
        this.distheneConfiguration = distheneConfiguration;
        this.rollups = rollups;
        this.bus = bus;
        bus.subscribe(this);

        for (Rollup rollup : rollups) {
            if (maxRollup == null || maxRollup.getRollup() < rollup.getRollup()) {
                maxRollup = rollup;
            }
        }

        scheduler.scheduleAtFixedRate(this::flush, 60 - ((System.currentTimeMillis() / 1000L) % 60), RATE, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unused")
    @Handler()
    public void handle(MetricStoreEvent metricStoreEvent) {
        if (rollups.size() > 0 && maxRollup.getRollup() > metricStoreEvent.getMetric().getRollup()) {
            aggregate(metricStoreEvent.getMetric());
        }
    }

    private ConcurrentMap<MetricKey, AverageRecord> getTimestampMap(long timestamp) {
        ConcurrentMap<MetricKey, AverageRecord> timestampMap = accumulator.get(timestamp);
        if (timestampMap == null) {
            ConcurrentMap<MetricKey, AverageRecord> newTimestampMap = new ConcurrentHashMap<>();
            timestampMap = accumulator.putIfAbsent(timestamp, newTimestampMap);
            if (timestampMap == null) {
                timestampMap = newTimestampMap;
            }
        }

        return timestampMap;
    }

    private AverageRecord getAverageRecord(ConcurrentMap<MetricKey, AverageRecord> map, MetricKey metricKey) {
        AverageRecord averageRecord = map.get(metricKey);
        if (averageRecord == null) {
            AverageRecord newAverageRecord = new AverageRecord();
            averageRecord = map.putIfAbsent(metricKey, newAverageRecord);
            if (averageRecord == null) {
                averageRecord = newAverageRecord;
            }
        }

        return averageRecord;
    }

    private void aggregate(Metric metric) {
        for (Rollup rollup : rollups) {
            long timestamp = getRollupTimestamp(metric.getTimestamp(), rollup);
            ConcurrentMap<MetricKey, AverageRecord> timestampMap = getTimestampMap(timestamp);
            MetricKey destinationMetricKey = new MetricKey(
                    metric.getTenant(), metric.getPath(),
                    rollup.getRollup(), rollup.getPeriod(),
                    timestamp);
            AverageRecord averageRecord = getAverageRecord(timestampMap, destinationMetricKey);
            averageRecord.addValue(metric.getValue());
        }
    }

    private void flush() {
        Collection<Metric> metricsToFlush = new ArrayList<>();

        // Get timestamps to flush
        Set<Long> timestampsToFlush = new HashSet<>(accumulator.headMap(DateTime.now(DateTimeZone.UTC).getMillis() / 1000 - distheneConfiguration.getCarbon().getAggregatorDelay() * 2L).keySet());

        logger.trace("There are " + timestampsToFlush.size() + " timestamps to flush");

        for (Long timestamp : timestampsToFlush) {
            ConcurrentMap<MetricKey, AverageRecord> timestampMap = accumulator.remove(timestamp);

            // double check just in case
            if (timestampMap != null) {
                logger.trace("Adding rollup flush for time: " + (new DateTime(timestamp * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");
                logger.trace("Will flush " + timestampMap.size() + " metrics");

                for (Map.Entry<MetricKey, AverageRecord> entry : timestampMap.entrySet()) {
                    metricsToFlush.add(new Metric(entry.getKey(), entry.getValue().getAverage()));
                }
                logger.trace("Done adding rollup flush for time: " + (new DateTime(timestamp * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");
            }
        }

        // do the flush asynchronously
        if (metricsToFlush.size() > 0) {
            logger.trace("Flushing total of " + metricsToFlush.size() + " metrics");

            CompletableFuture.supplyAsync((Supplier<Void>) () -> {
                doFlush(metricsToFlush, getFlushRateLimiter(metricsToFlush.size()));
                return null;
            }, flusher).whenComplete((o, error) -> {
                if (error != null) {
                    logger.error(error);
                } else {
                    logger.trace("Done flushing total of " + metricsToFlush.size() + " metrics");
                }
            });
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private RateLimiter getFlushRateLimiter(int currentBatch) {
        /*
        The idea is that we'd like to be able to process ALL the contents of accumulator in 1/2 time till next rollup time.
        Doing so, we hope to never limit as much as to saturate the accumulator and to heavily fall back
         */

        // Get the smallest rollup - we can never get here if there are no rollups at all
        Rollup rollup = rollups.get(0);

        long timestamp = System.currentTimeMillis() / 1000;
        // Get the deadline - next rollup border
        long deadline = getRollupTimestamp(timestamp, rollup);

        // Just in case
        if (deadline - timestamp <= 0) return null;

        // 100 is an arbitrary small number here
        double rate = Math.max(100, 2L * currentBatch / (deadline - timestamp));

        //noinspection UnstableApiUsage
        return RateLimiter.create(rate);
    }

    @SuppressWarnings("UnstableApiUsage")
    private void doFlush(Collection<Metric> metricsToFlush, RateLimiter rateLimiter) {
        // We'd like to feed metrics in a more gentle manner here but not allowing the queue to grow.

        logger.debug("Flushing rollup metrics (" + metricsToFlush.size() + ")");
        if (rateLimiter != null) {
            logger.debug("QPS is limited to " + (long) rateLimiter.getRate());
        }

        for (Metric metric : metricsToFlush) {
            if (!shuttingDown && rateLimiter != null) {
                rateLimiter.acquire();
            }
            bus.post(new MetricStoreEvent(metric)).now();
        }
    }

    public synchronized void shutdown() {
        // disable rate limiters
        shuttingDown = true;

        scheduler.shutdown();

        Collection<Metric> metricsToFlush = new ArrayList<>();

        for (Map.Entry<Long, ConcurrentMap<MetricKey, AverageRecord>> entry : accumulator.entrySet()) {
            for (Map.Entry<MetricKey, AverageRecord> innerEntry : entry.getValue().entrySet()) {
                metricsToFlush.add(new Metric(innerEntry.getKey(), innerEntry.getValue().getAverage()));
            }
        }
        // When shutting down we'd like to do flushing as fast as possible
        doFlush(metricsToFlush, null);
    }

    private static long getRollupTimestamp(long timestamp, Rollup rollup) {
        return ((long) Math.ceil(timestamp / (double) rollup.getRollup())) * rollup.getRollup();
    }

    private static class AverageRecord {
        private final AtomicDouble value = new AtomicDouble(0);
        private final AtomicInteger count = new AtomicInteger(0);

        void addValue(double value) {
            this.value.addAndGet(value);
            this.count.addAndGet(1);
        }

        double getAverage() {
            return value.get() / count.get();
        }
    }
}