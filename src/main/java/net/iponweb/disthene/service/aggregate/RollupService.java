package net.iponweb.disthene.service.aggregate;

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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    private final TreeMap<Long, Map<MetricKey, AggregationEntry>> accumulator = new TreeMap<>();

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


    public void aggregate(Metric metric) {

        synchronized (accumulator) {
            // Get next rollup for the one defined in metric
            for(Rollup rollup : rollups) {
                // Create aggregation entry (assuming average aggregator function)
                // determine timestamp for this metric/rollup
                long timestamp = getRollupTimestamp(metric.getTimestamp(), rollup);

                if (!accumulator.containsKey(timestamp)) {
                    accumulator.put(timestamp, new HashMap<MetricKey, AggregationEntry>());
                }
                Map<MetricKey, AggregationEntry> timestampMap = accumulator.get(timestamp);


                MetricKey destinationMetricKey = new MetricKey(
                        metric.getTenant(), metric.getPath(),
                        rollup.getRollup(), rollup.getPeriod(),
                        timestamp);

                if (timestampMap.containsKey(destinationMetricKey)) {
                    AggregationEntry destinationMetric = timestampMap.get(destinationMetricKey);
                    destinationMetric.addValue(metric.getValue());
                    timestampMap.put(destinationMetricKey, destinationMetric);
                } else {
                    timestampMap.put(destinationMetricKey, new AggregationEntry(destinationMetricKey, metric.getValue()));
                }
            }
        }
    }

    public void flush() {
        Collection<Metric> metricsToFlush = new ArrayList<>();

        synchronized (accumulator) {
            // check earliest timestamp map
            // We have to wait for sum metrics and stats longer - thus * 2
            if (accumulator.size() == 0 || (accumulator.firstKey() > DateTime.now(DateTimeZone.UTC).getMillis() * 1000 - distheneConfiguration.getCarbon().getAggregatorDelay() * 2)) {
                // nothing to do, just return
                return;
            }

            logger.debug("Adding rollup flush for time: " + (new DateTime(accumulator.firstKey() * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");

            // Get the earliest map
            for(AggregationEntry entry : accumulator.firstEntry().getValue().values()) {
                metricsToFlush.add(entry.getMetric());
            }

            // Remove it from accumulator
            accumulator.remove(accumulator.firstKey());

        }

        // call the flusher itself
        doFlush(metricsToFlush);
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

    public void shutdown() {
        scheduler.shutdown();

        Collection<Metric> metricsToFlush = new ArrayList<>();
        for(Map.Entry<Long, Map<MetricKey, AggregationEntry>> entry : accumulator.entrySet()) {
            for(AggregationEntry aggregationEntry : entry.getValue().values()) {
                metricsToFlush.add(aggregationEntry.getMetric());
            }
        }
        doFlush(metricsToFlush);
    }

    private static long getRollupTimestamp(long timestamp, Rollup rollup) {
        return ((long) Math.ceil(timestamp / (double) rollup.getRollup())) * rollup.getRollup();
    }

    private class AggregationEntry {
        private Metric metric;
        private int count = 0;

        private AggregationEntry(MetricKey key, double value) {
            metric = new Metric(key, value);
            count = 1;
        }

        public void addValue(double value) {
            metric.setValue((count * metric.getValue() + value) / (++count) );
        }

        public Metric getMetric() {
            return metric;
        }
    }
}
