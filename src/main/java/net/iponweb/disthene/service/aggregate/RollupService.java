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
import net.iponweb.disthene.util.NamedThreadFactory;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
public class RollupService {
    private static final String SCHEDULER_NAME = "distheneRollupAggregatorFlusher";
    private static final int RATE = 60;
    private volatile boolean shuttingDown = false;

    private Logger logger = Logger.getLogger(RollupService.class);


    private MBassador<DistheneEvent> bus;
    private DistheneConfiguration distheneConfiguration;
    private Rollup maxRollup;
    private List<Rollup> rollups;

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory(SCHEDULER_NAME));

    private final ConcurrentNavigableMap<Long, ConcurrentMap<MetricKey, AverageRecord>> accumulator = new ConcurrentSkipListMap<>();

    public RollupService(MBassador<DistheneEvent> bus, DistheneConfiguration distheneConfiguration, List<Rollup> rollups) {
        this.distheneConfiguration = distheneConfiguration;
        this.rollups = rollups;
        this.bus = bus;
        bus.subscribe(this);

        for(Rollup rollup : rollups) {
            if (maxRollup == null || maxRollup.getRollup() < rollup.getRollup()) {
                maxRollup = rollup;
            }
        }

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, RATE, RATE, TimeUnit.SECONDS);
    }

    @Handler(rejectSubtypes = false)
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

    public void aggregate(Metric metric) {
        for(Rollup rollup : rollups) {
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

    public void flush() {
        Collection<Metric> metricsToFlush = new ArrayList<>();
        Map<Integer, MutableInt> rollupCounters = new HashMap<>();
        for(Rollup rollup : rollups) {
            rollupCounters.put(rollup.getRollup(), new MutableInt(0));
        }

        while(accumulator.size() > 0 && (accumulator.firstKey() < DateTime.now(DateTimeZone.UTC).getMillis() / 1000 - distheneConfiguration.getCarbon().getAggregatorDelay() * 2)) {
            logger.debug("Adding rollup flush for time: " + (new DateTime(accumulator.firstKey() * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");

            // Get the earliest map
            ConcurrentMap<MetricKey, AverageRecord> timestampMap = accumulator.pollFirstEntry().getValue();

            for(Map.Entry<MetricKey, AverageRecord> entry : timestampMap.entrySet()) {
                metricsToFlush.add(new Metric(entry.getKey(), entry.getValue().getAverage()));
                rollupCounters.get(entry.getKey().getRollup()).increment();
            }
        }

        if (metricsToFlush.size() > 0) {
            doFlush(metricsToFlush, getRateLimiters(rollupCounters));
        }
    }

    private void doFlush(Collection<Metric> metricsToFlush, Map<Integer, RateLimiter> rateLimiters) {
        // We'd like to feed metrics in a more gentle manner here but not allowing the queue to grow.

        logger.debug("Flushing rollup metrics (" + metricsToFlush.size() + ")");

        for (Map.Entry<Integer, RateLimiter> rateLimiterEntry : rateLimiters.entrySet()) {
            logger.debug("Will limit qps to " + (long) rateLimiterEntry.getValue().getRate() + " for " + rateLimiterEntry.getKey() + " seconds rollups");
        }

        boolean rateLimitersEnabled = true;

        for(Metric metric : metricsToFlush) {
            if (rateLimitersEnabled && isShuttingDown()) {
                rateLimitersEnabled = false;
                logger.info("Got shutdown signal while flushing. Disabling rate limiters");
            }

            if (rateLimitersEnabled && rateLimiters.containsKey(metric.getRollup())) {
                rateLimiters.get(metric.getRollup()).acquire();
            }
            bus.post(new MetricStoreEvent(metric)).now();
        }
    }

    private boolean isShuttingDown() {
        return shuttingDown;
    }

    public synchronized void shutdown() {
        // disable rate limiters
        shuttingDown = true;
        scheduler.shutdown();

        Collection<Metric> metricsToFlush = new ArrayList<>();

        for(Map.Entry<Long, ConcurrentMap<MetricKey, AverageRecord>> entry : accumulator.entrySet()) {
            for(Map.Entry<MetricKey, AverageRecord> innerEntry : entry.getValue().entrySet()) {
                metricsToFlush.add(new Metric(innerEntry.getKey(), innerEntry.getValue().getAverage()));
            }
        }
        // When shutting down we'd like to do flushing as fast as possible
        doFlush(metricsToFlush, Collections.<Integer, RateLimiter>emptyMap());
    }

    private Map<Integer, RateLimiter> getRateLimiters(Map<Integer, MutableInt> rollupCounters) {
        // Create rate limiters for all the rollups
        // Let's be pessimistic and try to make it within 2/3 of aggregation period
        Map<Integer, RateLimiter> rateLimiters = new HashMap<>();
        for(Rollup rollup : rollups) {
            // create it only if we've seen at least one metric for this rollup
            if (rollupCounters.get(rollup.getRollup()).intValue() > 0) {
                long qps = Math.round((3.0 * rollupCounters.get(rollup.getRollup()).intValue()) / (2.0 * rollup.getRollup()));
                rateLimiters.put(rollup.getRollup(), RateLimiter.create(qps));
            }
        }

        return rateLimiters;
    }

    private static long getRollupTimestamp(long timestamp, Rollup rollup) {
        return ((long) Math.ceil(timestamp / (double) rollup.getRollup())) * rollup.getRollup();
    }

    private class AverageRecord {
        private AtomicDouble value = new AtomicDouble(0);
        private AtomicInteger count = new AtomicInteger(0);

        public void addValue(double value) {
            this.value.addAndGet(value);
            this.count.addAndGet(1);
        }

        public double getAverage() {
            return value.get() / count.get();
        }
    }
}
