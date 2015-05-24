package net.iponweb.disthene.service.aggregate;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.AggregationRule;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricKey;
import net.iponweb.disthene.config.AggregationConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.service.blacklist.BlackList;
import net.iponweb.disthene.service.events.MetricIndexEvent;
import net.iponweb.disthene.service.events.MetricReceivedEvent;
import net.iponweb.disthene.service.events.MetricStoreEvent;
import net.iponweb.disthene.service.util.NameThreadFactory;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
// todo: handle names other than <data>
public class SumAggregator {
    private static final String SCHEDULER_NAME = "distheneSumAggregatorFlusher";
    private static final int RATE = 60;

    private Logger logger = Logger.getLogger(SumAggregator.class);

    private MBassador bus;
    private DistheneConfiguration distheneConfiguration;
    private AggregationConfiguration aggregationConfiguration;
    private BlackList blackList;
    private final TreeMap<DateTime, Map<MetricKey, Metric>> accumulator = new TreeMap<>();

    public SumAggregator(MBassador bus, DistheneConfiguration distheneConfiguration, AggregationConfiguration aggregationConfiguration, BlackList blackList) {
        this.bus = bus;
        this.distheneConfiguration = distheneConfiguration;
        this.aggregationConfiguration = aggregationConfiguration;
        this.blackList = blackList;
        bus.subscribe(this);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NameThreadFactory(SCHEDULER_NAME));
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, RATE, RATE, TimeUnit.SECONDS);

    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricReceivedEvent metricReceivedEvent) {
        aggregate(metricReceivedEvent.getMetric());
    }


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
            if (!blackList.isBlackListed(metric)) {
                bus.post(new MetricStoreEvent(metric)).now();
                bus.post(new MetricIndexEvent(metric)).now();
            }
        }

    }
}
