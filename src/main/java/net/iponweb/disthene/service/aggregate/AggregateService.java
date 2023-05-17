package net.iponweb.disthene.service.aggregate;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.common.util.concurrent.RateLimiter;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricKey;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.config.Rollup;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricAggregateEvent;
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
 * @author PAAS <paas@cloud.dreamlab.pl>
 */
@Listener(references= References.Strong)
public class AggregateService {
    private static final String SCHEDULER_NAME = "distheneAggregatorFlusher";
    private static final int RATE = 10;
    private volatile boolean shuttingDown = false;

    private Logger logger = Logger.getLogger(AggregateService.class);

    private MBassador<DistheneEvent> bus;
    private DistheneConfiguration distheneConfiguration;
    private Rollup baseRollup;

    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1, new NamedThreadFactory(SCHEDULER_NAME));

    private final ConcurrentNavigableMap<Long, ConcurrentMap<MetricKey, AverageRecord>> accumulator = new ConcurrentSkipListMap<>();

    public AggregateService(MBassador<DistheneEvent> bus, DistheneConfiguration distheneConfiguration) {
        this.distheneConfiguration = distheneConfiguration;
        this.baseRollup = distheneConfiguration.getCarbon().getBaseRollup();
        this.bus = bus;
        bus.subscribe(this);

        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, 60 - ((System.currentTimeMillis() / 1000L) % 60), RATE, TimeUnit.SECONDS);
    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricAggregateEvent metricAggregateEvent) {
        aggregate(metricAggregateEvent.getMetric());
    }

    private ConcurrentMap<MetricKey, AverageRecord> getTimestampMap(long timestamp) {
        /*
	 Get or create new element of map to hold average values for given timestamp.
	 */
        ConcurrentMap<MetricKey, AverageRecord> timestampMap = accumulator.get(timestamp);
        if (timestampMap == null) {
            timestampMap = new ConcurrentHashMap<>();
            accumulator.putIfAbsent(timestamp, timestampMap);
        }

        return timestampMap;
    }

    private AverageRecord getAverageRecord(ConcurrentMap<MetricKey, AverageRecord> map, MetricKey metricKey) {
        AverageRecord averageRecord = map.get(metricKey);
        if (averageRecord == null) {
            averageRecord = new AverageRecord();
            map.putIfAbsent(metricKey, averageRecord);
        }

        return averageRecord;
    }

    private void aggregate(Metric metric) {
        ConcurrentMap<MetricKey, AverageRecord> timestampMap = getTimestampMap(metric.getTimestamp());
        MetricKey destinationMetricKey = new MetricKey(
                metric.getTenant(), metric.getPath(),
                metric.getRollup(), metric.getPeriod(),
                metric.getTimestamp());
        getAverageRecord(timestampMap, destinationMetricKey).addValue(metric.getValue());
    }

    private void flush() {
        Collection<Metric> metricsToFlush = new ArrayList<>();
        double sendAfterTimestamp = DateTime.now(DateTimeZone.UTC).getMillis() / 1000 - distheneConfiguration.getCarbon().getAggregatorDelay();
        while(accumulator.size() > 0 && (accumulator.firstKey() < sendAfterTimestamp)) {
            logger.debug("Adding base rollup flush for time: " + (new DateTime(accumulator.firstKey() * 1000)) + " (current time is " + DateTime.now(DateTimeZone.UTC) + ")");

            // Get the earliest map
            ConcurrentMap<MetricKey, AverageRecord> timestampMap = accumulator.pollFirstEntry().getValue();

            for(Map.Entry<MetricKey, AverageRecord> entry : timestampMap.entrySet()) {
                metricsToFlush.add(new Metric(entry.getKey(), entry.getValue().getAverage()));
            }
        }

        if (metricsToFlush.size() > 0) {
            doFlush(metricsToFlush, getFlushRateLimiter(metricsToFlush.size()));
        }
    }

    private RateLimiter getFlushRateLimiter(int currentBatch) {
        /*
        The idea is that we'd like to be able to process ALL the contents of accumulator in 1/2 time till next rollup time.
        Doing so, we hope to never limit as much as to saturate the accumulator and to heavily fall back
         */

        // 100 is an arbitrary small number here
        double rate = Math.max(100, 2 * currentBatch / baseRollup.getRollup());

        return RateLimiter.create(rate);

    }

    private void doFlush(Collection<Metric> metricsToFlush, RateLimiter rateLimiter) {
        // We'd like to feed metrics in a more gentle manner here but not allowing the queue to grow.

        logger.debug("Flushing rollup metrics (" + metricsToFlush.size() + ")");
        if (rateLimiter != null) {
            logger.debug("QPS is limited to " + rateLimiter.getRate());
        }

        for(Metric metric : metricsToFlush) {
            if (!shuttingDown && rateLimiter != null) {
                rateLimiter.acquire();
            }
            bus.post(new MetricStoreEvent(metric)).now();
        }
    }

    public synchronized void shutdown() {
        // disable rate limiters
        shuttingDown = true;

        scheduler.shutdown();

        Collection<Metric> metricsToFlush = new ArrayList<>();

        for(Map.Entry<Long, ConcurrentMap<MetricKey, AverageRecord>> entry : accumulator.entrySet()) {
            for(Map.Entry<MetricKey, AverageRecord> innerEntry : entry.getValue().entrySet()) {
                metricsToFlush.add(new Metric(innerEntry.getKey(), innerEntry.getValue().getAverage()));
            }
        }
        // When shutting down we'd like to do flushing as fast as possible
        doFlush(metricsToFlush, null);
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
