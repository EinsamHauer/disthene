package net.iponweb.disthene.service.aggregate;

import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Andrei Ivanov
 */
public class AggregationFlusher {
    private Logger logger = Logger.getLogger(AggregationFlusher.class);

    private static final int RATE = 10;

    private final ScheduledExecutorService aggregatorScheduler = Executors.newScheduledThreadPool(1);
    private final ScheduledExecutorService rollupAggregatorScheduler = Executors.newScheduledThreadPool(1);

    public AggregationFlusher(final Aggregator aggregator, final Aggregator rollupAggregator) {
        // Start the execution
        // We will schedule every 10 seconds, but real work will probably be done every aggregatorDelay seconds
        aggregatorScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                aggregator.flush();
            }
        }, RATE, RATE, TimeUnit.SECONDS);

        rollupAggregatorScheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                rollupAggregator.flush();
            }
        }, RATE, RATE, TimeUnit.SECONDS);
    }

    public void start() {
    }

}
