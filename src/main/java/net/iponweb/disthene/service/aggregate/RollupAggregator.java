package net.iponweb.disthene.service.aggregate;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricKey;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.service.events.DistheneEvent;
import net.iponweb.disthene.service.events.MetricStoreEvent;
import net.iponweb.disthene.service.util.NameThreadFactory;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
public class RollupAggregator {
    private static final String SCHEDULER_NAME = "distheneRollupAggregatorFlusher";
    private static final int RATE = 1;

    private Logger logger = Logger.getLogger(RollupAggregator.class);


    private MBassador<DistheneEvent> bus;
    private DistheneConfiguration distheneConfiguration;
    private Rollup maxRollup;
    private List<Rollup> rollups;

    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NameThreadFactory(SCHEDULER_NAME));

    private final TreeMap<DateTime, Map<MetricKey, AggregationEntry>> accumulator = new TreeMap<>();

    public RollupAggregator(MBassador<DistheneEvent> bus, DistheneConfiguration distheneConfiguration, List<Rollup> rollups) {
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
                DateTime timestamp = getRollupTimestamp(metric.getTimestamp(), rollup);

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
            if (accumulator.size() == 0 || !accumulator.firstKey().isBefore(DateTime.now().minusSeconds(distheneConfiguration.getCarbon().getAggregatorDelay() * 2))) {
                // nothing to do, just return
                return;
            }

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
        for(Metric metric : metricsToFlush) {
            bus.post(new MetricStoreEvent(metric)).now();
        }

    }

    public void shutdown() {
        scheduler.shutdown();

        Collection<Metric> metricsToFlush = new ArrayList<>();
        for(Map.Entry<DateTime, Map<MetricKey, AggregationEntry>> entry : accumulator.entrySet()) {
            for(AggregationEntry aggregationEntry : entry.getValue().values()) {
                metricsToFlush.add(aggregationEntry.getMetric());
            }
        }
        doFlush(metricsToFlush);
    }



    private static DateTime getRollupTimestamp(DateTime dt, Rollup rollup) {
        int minutes = (int) (Math.ceil(dt.getMinuteOfHour() / (double) (rollup.getRollup() / 60)) * 15);
        return dt.withMinuteOfHour(0).plusMinutes(minutes);
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
