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

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private DistheneConfiguration distheneConfiguration;
    private Aggregator aggregator;

    public AggregationFlusher(DistheneConfiguration distheneConfiguration, Aggregator aggregator) {
        this.distheneConfiguration = distheneConfiguration;
        this.aggregator = aggregator;
    }

    public void start() {
        // Start the execution
        // We will schedule every 10 seconds, but real work will probably be done every aggregatorDelay seconds
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.debug("Invoke flusher");
                aggregator.flush();
            }
        }, RATE, RATE, TimeUnit.SECONDS);
    }

}
