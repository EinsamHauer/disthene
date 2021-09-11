package net.iponweb.disthene.service.aggregate;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.AggregationRule;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricKey;
import net.iponweb.disthene.config.AggregationConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.service.blacklist.BlacklistService;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricReceivedEvent;
import net.iponweb.disthene.events.MetricStoreEvent;
import net.iponweb.disthene.util.NamedThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.regex.Matcher;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
// todo: handle names other than <data>
public class SumService {
    private static final String SCHEDULER_NAME = "distheneSumAggregatorFlusherScheduler";
    private static final String FLUSHER_NAME = "distheneRollupAggregatorFlusher";
    private static final int RATE = 60;
    private volatile boolean shuttingDown = false;

    private static final Logger logger = LogManager.getLogger(SumService.class);

    private final MBassador<DistheneEvent> bus;
    private final DistheneConfiguration distheneConfiguration;
    private AggregationConfiguration aggregationConfiguration;
    private final BlacklistService blacklistService;
    private final ConcurrentNavigableMap<Long, ConcurrentMap<MetricKey, AtomicDouble>> accumulator = new ConcurrentSkipListMap<>();

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory(SCHEDULER_NAME));
    private final ExecutorService flusher = Executors.newCachedThreadPool(new NamedThreadFactory(FLUSHER_NAME));

    public SumService(MBassador<DistheneEvent> bus, DistheneConfiguration distheneConfiguration, AggregationConfiguration aggregationConfiguration, BlacklistService blacklistService) {
        this.bus = bus;
        this.distheneConfiguration = distheneConfiguration;
        this.aggregationConfiguration = aggregationConfiguration;
        this.blacklistService = blacklistService;
        bus.subscribe(this);


        scheduler.scheduleAtFixedRate(this::flush, 60 - ((System.currentTimeMillis() / 1000L) % 60), RATE, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unused")
    @Handler()
    public void handle(MetricReceivedEvent metricReceivedEvent) {
        aggregate(metricReceivedEvent.getMetric());
    }

    private ConcurrentMap<MetricKey, AtomicDouble> getTimestampMap(long timestamp) {
        ConcurrentMap<MetricKey, AtomicDouble> timestampMap = accumulator.get(timestamp);
        if (timestampMap == null) {
            ConcurrentMap<MetricKey, AtomicDouble> newTimestampMap = new ConcurrentHashMap<>();
            timestampMap = accumulator.putIfAbsent(timestamp, newTimestampMap);
            if (timestampMap == null) {
                timestampMap = newTimestampMap;
            }
        }

        return timestampMap;
    }

    private AtomicDouble getMetricValue(ConcurrentMap<MetricKey, AtomicDouble> map, MetricKey metricKey) {
        AtomicDouble value = map.get(metricKey);
        if (value == null) {
            AtomicDouble newValue = new AtomicDouble(0);
            value = map.putIfAbsent(metricKey, newValue);
            if (value == null) {
                value = newValue;
            }
        }

        return value;
    }

    private void aggregate(Metric metric) {
        List<AggregationRule> rules = aggregationConfiguration.getRules().get(metric.getTenant());

        if (rules == null) {
            return;
        }

        ConcurrentMap<MetricKey, AtomicDouble> timestampMap = getTimestampMap(metric.getTimestamp());

        for(AggregationRule rule : rules) {
            Matcher m = rule.getSource().matcher(metric.getPath());
            if (m.matches()) {
                String destinationPath = rule.getPrefix() + m.group("data");
                MetricKey destinationKey = new MetricKey(metric.getTenant(), destinationPath, metric.getRollup(), metric.getPeriod(), metric.getTimestamp());
                getMetricValue(timestampMap, destinationKey).addAndGet(metric.getValue());
            }

        }
    }

    private void flush() {
        Collection<Metric> metricsToFlush = new ArrayList<>();

        // Get timestamps to flush
        Set<Long> timestampsToFlush = new HashSet<>(accumulator.headMap(DateTime.now(DateTimeZone.UTC).getMillis() / 1000 - distheneConfiguration.getCarbon().getAggregatorDelay()).keySet());
        logger.trace("There are " + timestampsToFlush.size() + " timestamps to flush");

        for (Long timestamp : timestampsToFlush) {
            ConcurrentMap<MetricKey, AtomicDouble> timestampMap = accumulator.remove(timestamp);

            // double check just in case
            if (timestampMap != null) {
                logger.trace("Adding sum flush for time: " + (new DateTime(timestamp * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");
                logger.trace("Will flush " + timestampMap.size() + " metrics");

                for (Map.Entry<MetricKey, AtomicDouble> entry : timestampMap.entrySet()) {
                    metricsToFlush.add(new Metric(entry.getKey(), entry.getValue().get()));
                }
                logger.trace("Done adding sum flush for time: " + (new DateTime(timestamp * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");
            }
        }

        // do the flush asynchronously
        if (metricsToFlush.size() > 0) {
            logger.trace("Flushing total of " + metricsToFlush.size() + " metrics");

            CompletableFuture.supplyAsync((Supplier<Void>) () -> {
                doFlush(metricsToFlush, getFlushRateLimiter(metricsToFlush.size()));
                return null;
            }, flusher).whenComplete((o, error) -> {
                if (error != null) {
                    logger.error(error);
                } else {
                    logger.trace("Done flushing total of " + metricsToFlush.size() + " metrics");
                }
            });
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private RateLimiter getFlushRateLimiter(int currentBatch) {
        /*
        The idea is that we'd like to be able to process ALL the contents of the batch in 1/2 of rollup.
        Doing so, we hope to never limit as much as to saturate the accumulator and to heavily fall back
         */

        // Get the smallest rollup - we can never get here if there are no rollups at all
        Rollup rollup = distheneConfiguration.getCarbon().getBaseRollup();

        // 100 is an arbitrary small number here
        double rate = Math.max(100, 2 * currentBatch / rollup.getRollup());

        return RateLimiter.create(rate);
    }

    @SuppressWarnings("UnstableApiUsage")
    private void doFlush(Collection<Metric> metricsToFlush, RateLimiter rateLimiter) {
        logger.debug("Flushing metrics (" + metricsToFlush.size() + ")");

        if (rateLimiter != null) {
            logger.debug("QPS is limited to " + (long) rateLimiter.getRate());
        }

        for(Metric metric : metricsToFlush) {
            if (!blacklistService.isBlackListed(metric)) {
                if (!shuttingDown && rateLimiter != null) {
                    rateLimiter.acquire();
                }
                bus.post(new MetricStoreEvent(metric)).now();
            }
        }
    }

    public synchronized void shutdown() {
        // disable rate limiters
        shuttingDown = true;

        scheduler.shutdown();

        Collection<Metric> metricsToFlush = new ArrayList<>();
        for(Map.Entry<Long, ConcurrentMap<MetricKey, AtomicDouble>> entry : accumulator.entrySet()) {
            for(Map.Entry<MetricKey, AtomicDouble> innerEntry : entry.getValue().entrySet()) {
                metricsToFlush.add(new Metric(innerEntry.getKey(), innerEntry.getValue().get()));
            }
        }
        doFlush(metricsToFlush, null);
    }

    public void setAggregationConfiguration(AggregationConfiguration aggregationConfiguration) {
        this.aggregationConfiguration = aggregationConfiguration;
    }
}