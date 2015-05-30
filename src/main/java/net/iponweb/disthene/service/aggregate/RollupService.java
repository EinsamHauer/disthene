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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
public class RollupService {
    private static final String SCHEDULER_NAME = "distheneRollupAggregatorFlusher";
    private static final int RATE = 1;

    private Logger logger = Logger.getLogger(RollupService.class);


    private MBassador<DistheneEvent> bus;
    private DistheneConfiguration distheneConfiguration;
    private Rollup maxRollup;
    private List<Rollup> rollups;

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory(SCHEDULER_NAME));

    private final ConcurrentNavigableMap<Long, ConcurrentMap<MetricKey, AggregationEntry>> accumulator = new ConcurrentSkipListMap<>();

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

    private ConcurrentMap<MetricKey, AggregationEntry> getTimestampMap(long timestamp) {
        ConcurrentMap<MetricKey, AggregationEntry> timestampMap = accumulator.get(timestamp);
        if (timestampMap == null) {
            ConcurrentMap<MetricKey, AggregationEntry> newTimestampMap = new ConcurrentHashMap<>();
            timestampMap = accumulator.putIfAbsent(timestamp, newTimestampMap);
            if (timestampMap == null) {
                timestampMap = newTimestampMap;
            }
        }

        return timestampMap;
    }

    private AggregationEntry getAggregationEntry(ConcurrentMap<MetricKey, AggregationEntry> map, MetricKey metricKey) {
        AggregationEntry aggregationEntry = map.get(metricKey);
        if (aggregationEntry == null) {
            AggregationEntry newAggregationEntry = new AggregationEntry();
            aggregationEntry = map.putIfAbsent(metricKey, newAggregationEntry);
            if (aggregationEntry == null) {
                aggregationEntry = newAggregationEntry;
            }
        }

        return aggregationEntry;
    }

    public void aggregate(Metric metric) {
        for(Rollup rollup : rollups) {
            long timestamp = getRollupTimestamp(metric.getTimestamp(), rollup);
            ConcurrentMap<MetricKey, AggregationEntry> timestampMap = getTimestampMap(timestamp);
            MetricKey destinationMetricKey = new MetricKey(
                    metric.getTenant(), metric.getPath(),
                    rollup.getRollup(), rollup.getPeriod(),
                    timestamp);
            AggregationEntry aggregationEntry = getAggregationEntry(timestampMap, destinationMetricKey);
            aggregationEntry.addValue(metric.getValue());
        }
    }

    public void flush() {
        Collection<Metric> metricsToFlush = new ArrayList<>();

        while(accumulator.size() > 0 && (accumulator.firstKey() < DateTime.now(DateTimeZone.UTC).getMillis() / 1000 - distheneConfiguration.getCarbon().getAggregatorDelay() * 2)) {
            logger.debug("Adding rollup flush for time: " + (new DateTime(accumulator.firstKey() * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");

            // Get the earliest map
            ConcurrentMap<MetricKey, AggregationEntry> timestampMap = accumulator.pollFirstEntry().getValue();

            for(Map.Entry<MetricKey, AggregationEntry> entry : timestampMap.entrySet()) {
                metricsToFlush.add(new Metric(entry.getKey(), entry.getValue().getAverage()));
            }
        }

        if (metricsToFlush.size() > 0) {
            doFlush(metricsToFlush);
        }
    }

    private synchronized void doFlush(Collection<Metric> metricsToFlush) {
        logger.debug("Flushing rollup metrics (" + metricsToFlush.size() + ")");
        //todo: remove this in final version
        if (metricsToFlush.size() < 10) {
            for(Metric metric : metricsToFlush) {
                logger.debug(metric);
            }
        }
        for(Metric metric : metricsToFlush) {
            bus.post(new MetricStoreEvent(metric)).now();
        }
    }

    //todo: correct shutdown
    public void shutdown() {
        scheduler.shutdown();

/*
        Collection<Metric> metricsToFlush = new ArrayList<>();
        for(Map.Entry<Long, Map<MetricKey, AggregationEntry>> entry : accumulator.entrySet()) {
            for(AggregationEntry aggregationEntry : entry.getValue().values()) {
                metricsToFlush.add(aggregationEntry.getMetric());
            }
        }
        doFlush(metricsToFlush);
*/
    }

    private static long getRollupTimestamp(long timestamp, Rollup rollup) {
        return ((long) Math.ceil(timestamp / (double) rollup.getRollup())) * rollup.getRollup();
    }

    private class AggregationEntry {
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
