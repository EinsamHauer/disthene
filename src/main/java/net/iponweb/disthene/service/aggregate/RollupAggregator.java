package net.iponweb.disthene.service.aggregate;

import com.google.common.util.concurrent.AtomicDouble;
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
import java.util.concurrent.*;

/**
 * @author Andrei Ivanov
 */
// todo: aggregate to more than 1 rollup
public class RollupAggregator {

    private Logger logger = Logger.getLogger(RollupAggregator.class);
    private DistheneConfiguration distheneConfiguration;
    private MetricStore metricStore;
    private List<Rollup> rollups;
    private Rollup theRollup = null;

//    private final TreeMap<DateTime, Map<MetricKey, AggregationEntry>> accumulator = new TreeMap<>();

    // time->tenant->path->value
    private ConcurrentSkipListMap<Long, ConcurrentMap<String, ConcurrentMap<String, AggregationEntry>>> acc = new ConcurrentSkipListMap<>();

    public RollupAggregator(DistheneConfiguration distheneConfiguration, List<Rollup> rollups, MetricStore metricStore) {
        this.distheneConfiguration = distheneConfiguration;
        this.rollups = rollups;
        this.metricStore = metricStore;
        if (rollups.size() > 0) {
            theRollup = rollups.get(0);
        }

        ScheduledExecutorService flushScheduler = Executors.newScheduledThreadPool(1);
        flushScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    public void aggregate(String tenant, String path, long time, double value) {
        long now = DateTime.now(DateTimeZone.UTC).getMillis() / 1000;
        if(time < now - distheneConfiguration.getCarbon().getAggregatorDelay()) {
            logger.debug("Discarding metric - too late (" + now + " vs " + time + ")");
            return;
        }

        long rollupTime = getRollupTime(time, theRollup);

        acc.putIfAbsent(rollupTime, new ConcurrentHashMap<String, ConcurrentMap<String, AggregationEntry>>());
        acc.get(rollupTime).putIfAbsent(tenant, new ConcurrentHashMap<String, AggregationEntry>());
        acc.get(rollupTime).get(tenant).putIfAbsent(path, new AggregationEntry());

        acc.get(rollupTime).get(tenant).get(path).addValue(value);
/*
        synchronized (acc.get(rollupTime).get(tenant).get(path)) {
            acc.get(rollupTime).get(tenant).get(path).addValue(value);
        }
*/

    }

    private void flush() {

    }

    private static long getRollupTime(long time, Rollup rollup) {
        return ((int)Math.ceil(time / (double)rollup.getRollup())) * rollup.getRollup();
    }

    private class AggregationEntry {
        private double value;
        private int count = 0;

        private AggregationEntry() {
            value = 0;
            count = 1;
        }

        public void addValue(double value) {
            this.value =  (count * value + this.value) / (++count);
        }

        public double getValue() {
            return value;
        }
    }

/*
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
*/

/*
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

    @Override
    public void setGeneralStore(GeneralStore generalStore) {
        // we don't need it here
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
*/

}
