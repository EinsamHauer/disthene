package net.iponweb.disthene.service.aggregate;

import com.google.common.util.concurrent.AtomicDouble;
import net.iponweb.disthene.bean.AggregationRule;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricKey;
import net.iponweb.disthene.config.AggregationConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.service.general.GeneralStore;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;

/**
 * @author Andrei Ivanov
 */
public class SumAggregator {
    private Logger logger = Logger.getLogger(SumAggregator.class);


    private DistheneConfiguration distheneConfiguration;
    private AggregationConfiguration aggregationConfiguration;
    private GeneralStore generalStore;
    private final TreeMap<DateTime, Map<MetricKey, Metric>> accumulator = new TreeMap<>();

    // time->tenant->path->value
    private ConcurrentSkipListMap<Long, ConcurrentMap<String, ConcurrentMap<String, AtomicDouble>>> sums = new ConcurrentSkipListMap<>();

    public SumAggregator(DistheneConfiguration distheneConfiguration, AggregationConfiguration aggregationConfiguration) {
        this.distheneConfiguration = distheneConfiguration;
        this.aggregationConfiguration = aggregationConfiguration;

        ScheduledExecutorService flushScheduler = Executors.newScheduledThreadPool(1);
        flushScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    // todo: handle names other than <data>
    public synchronized void aggregate(String tenant, String path, long time, double value) {
        long now = DateTime.now(DateTimeZone.UTC).getMillis() / 1000;

        if(time < now - distheneConfiguration.getCarbon().getAggregatorDelay()) {
            logger.debug("Discarding metric - too late");
            return;
        }

        if (!aggregationConfiguration.getRules().containsKey(tenant)) {
            return;
        }

        sums.putIfAbsent(time, new ConcurrentHashMap<String, ConcurrentMap<String, AtomicDouble>>());
        sums.get(time).putIfAbsent(tenant, new ConcurrentHashMap<String, AtomicDouble>());

        for(AggregationRule rule : aggregationConfiguration.getRules().get(tenant)) {
            Matcher m = rule.getSource().matcher(path);
            if (m.matches()) {
                // destination path
                String aggregationPath = rule.getDestination().replace("<data>", m.group("data"));
                sums.get(time).get(tenant).putIfAbsent(aggregationPath, new AtomicDouble(0));
                try {
                    sums.get(time).get(tenant).get(aggregationPath).addAndGet(value);
                } catch (Exception e){
                    logger.error(e);
                    logger.error(sums.get(time).get(tenant).get(aggregationPath));

                }
            }
        }
    }

    public void flush() {
        if (sums.size() == 0) {
            return;
        }

        long now = DateTime.now(DateTimeZone.UTC).getMillis() / 1000;
        if(sums.firstKey() > now - distheneConfiguration.getCarbon().getAggregatorDelay()) {
            return;
        }

        Map.Entry<Long, ConcurrentMap<String, ConcurrentMap<String, AtomicDouble>>> metricsToFlushEntry = sums.pollFirstEntry();
        long time = metricsToFlushEntry.getKey();

        // todo: write metrics
        logger.debug("Flushing " + metricsToFlushEntry.getValue().get("test").size());


    }

}
