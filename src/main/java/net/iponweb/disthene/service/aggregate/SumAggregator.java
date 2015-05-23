package net.iponweb.disthene.service.aggregate;

import net.iponweb.disthene.bean.AggregationRule;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricKey;
import net.iponweb.disthene.config.AggregationConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.service.general.GeneralStore;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.*;
import java.util.regex.Matcher;

/**
 * @author Andrei Ivanov
 */
public class SumAggregator implements Aggregator {
    private Logger logger = Logger.getLogger(SumAggregator.class);


    private DistheneConfiguration distheneConfiguration;
    private AggregationConfiguration aggregationConfiguration;
    private GeneralStore generalStore;
    private final TreeMap<DateTime, Map<MetricKey, Metric>> accumulator = new TreeMap<>();

    public SumAggregator(DistheneConfiguration distheneConfiguration, AggregationConfiguration aggregationConfiguration) {
        this.distheneConfiguration = distheneConfiguration;
        this.aggregationConfiguration = aggregationConfiguration;
    }

    public void setGeneralStore(GeneralStore generalStore) {
        this.generalStore = generalStore;
    }

    // todo: handle names other than <data>
    @Override
    public void aggregate(Metric metric) {

        // Get aggregation rules
        List<AggregationRule> rules = aggregationConfiguration.getRules().get(metric.getTenant());

        if (rules == null) {
            return;
        }

        synchronized (accumulator) {
            // create entry for timestamp if needed
            if (!accumulator.containsKey(metric.getTimestamp())) {
                accumulator.put(metric.getTimestamp(), new HashMap<MetricKey, Metric>());
            }
            Map<MetricKey, Metric> timestampMap = accumulator.get(metric.getTimestamp());

            for(AggregationRule rule : rules) {
                Matcher m = rule.getSource().matcher(metric.getPath());
                if (m.matches()) {
                    // destination path
                    String destinationPath = rule.getDestination().replace("<data>", m.group("data"));
                    MetricKey destinationKey = new MetricKey(metric.getTenant(), destinationPath, metric.getRollup(), metric.getPeriod(), metric.getTimestamp());
                    if (timestampMap.containsKey(destinationKey)) {
                        Metric destinationMetric = timestampMap.get(destinationKey);
                        destinationMetric.setValue(destinationMetric.getValue() + metric.getValue());
                        timestampMap.put(destinationKey, destinationMetric);
                    } else {
                        timestampMap.put(destinationKey, new Metric(metric.getTenant(), destinationPath, metric.getRollup(), metric.getPeriod(), metric.getValue(), metric.getTimestamp()));
                    }

                }
            }
        }
    }

    public void flush() {
        Collection<Metric> metricsToFlush;
        synchronized (accumulator) {
            // check earliest timestamp map
            if (accumulator.size() == 0 || !accumulator.firstKey().isBefore(DateTime.now().minusSeconds(distheneConfiguration.getCarbon().getAggregatorDelay()))) {
                // nothing to do, just return
                return;
            }

            // Get the earliest map
            metricsToFlush = accumulator.firstEntry().getValue().values();
            // Remove it from accumulator
            accumulator.remove(accumulator.firstKey());

            // call the flusher itself
        }

        doFlush(metricsToFlush);
    }

    private synchronized void doFlush(Collection<Metric> metricsToFlush) {
        logger.debug("Flushing metrics (" + metricsToFlush.size() + ")");
        for(Metric metric : metricsToFlush) {
//            generalStore.store(metric);
        }

    }
}
