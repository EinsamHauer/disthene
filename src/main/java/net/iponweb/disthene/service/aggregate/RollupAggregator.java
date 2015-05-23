package net.iponweb.disthene.service.aggregate;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricKey;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.service.general.GeneralStore;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.*;

/**
 * @author Andrei Ivanov
 */
public class RollupAggregator implements Aggregator {

    private Logger logger = Logger.getLogger(RollupAggregator.class);
    private DistheneConfiguration distheneConfiguration;
    private MetricStore metricStore;
    private List<Rollup> rollups;

    private final TreeMap<DateTime, Map<MetricKey, AggregationEntry>> accumulator = new TreeMap<>();

    public RollupAggregator(DistheneConfiguration distheneConfiguration, List<Rollup> rollups, MetricStore metricStore) {
        this.distheneConfiguration = distheneConfiguration;
        this.rollups = rollups;
        this.metricStore = metricStore;
    }

    @Override
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

    @Override
    public void flush() {
        Collection<Metric> metricsToFlush = new ArrayList<>();

        synchronized (accumulator) {
            // check earliest timestamp map
            if (accumulator.size() == 0 || !accumulator.firstKey().isBefore(DateTime.now().minusSeconds(distheneConfiguration.getCarbon().getAggregatorDelay()))) {
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
            metricStore.store(metric);
        }

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
