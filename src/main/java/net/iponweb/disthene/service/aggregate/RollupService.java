package net.iponweb.disthene.service.aggregate;

import com.google.common.util.concurrent.AtomicDouble;
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
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
public class RollupService {
    private static final String SCHEDULER_NAME = "distheneRollupAggregatorFlusher";
    private static final int RATE = 60;

    private Logger logger = Logger.getLogger(RollupService.class);

    private MBassador<DistheneEvent> bus;
    private DistheneConfiguration distheneConfiguration;
    private Rollup maxRollup;
    private List<Rollup> rollups;

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory(SCHEDULER_NAME));

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
        }, 60 - ((System.currentTimeMillis() / 1000L) % 60), RATE, TimeUnit.SECONDS);
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

    private void aggregate(Metric metric) {
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

    private void flush() {
        Collection<Metric> metricsToFlush = new ArrayList<>();

        while(accumulator.size() > 0 && (accumulator.firstKey() < DateTime.now(DateTimeZone.UTC).getMillis() / 1000 - distheneConfiguration.getCarbon().getAggregatorDelay() * 2)) {
            logger.debug("Adding rollup flush for time: " + (new DateTime(accumulator.firstKey() * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");

            // Get the earliest map
            ConcurrentMap<MetricKey, AverageRecord> timestampMap = accumulator.pollFirstEntry().getValue();

            for(Map.Entry<MetricKey, AverageRecord> entry : timestampMap.entrySet()) {
                metricsToFlush.add(new Metric(entry.getKey(), entry.getValue().getAverage()));
            }
        }

        if (metricsToFlush.size() > 0) {
            doFlush(metricsToFlush);
        }
    }

    private void doFlush(Collection<Metric> metricsToFlush) {
        logger.debug("Flushing rollup metrics (" + metricsToFlush.size() + ")");

        for(Metric metric : metricsToFlush) {
            bus.post(new MetricStoreEvent(metric)).now();
        }
    }

    public synchronized void shutdown() {
        scheduler.shutdown();

        Collection<Metric> metricsToFlush = new ArrayList<>();

        for(Map.Entry<Long, ConcurrentMap<MetricKey, AverageRecord>> entry : accumulator.entrySet()) {
            for(Map.Entry<MetricKey, AverageRecord> innerEntry : entry.getValue().entrySet()) {
                metricsToFlush.add(new Metric(innerEntry.getKey(), innerEntry.getValue().getAverage()));
            }
        }

        doFlush(metricsToFlush);
    }

    private static long getRollupTimestamp(long timestamp, Rollup rollup) {
        return ((long) Math.ceil(timestamp / (double) rollup.getRollup())) * rollup.getRollup();
    }

    private class AverageRecord {
        private AtomicDouble value = new AtomicDouble(0);
        private AtomicInteger count = new AtomicInteger(0);

        void addValue(double value) {
            this.value.addAndGet(value);
            this.count.addAndGet(1);
        }

        double getAverage() {
            return value.get() / count.get();
        }
    }
}
