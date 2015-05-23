package net.iponweb.disthene.service.store;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.service.events.MetricReceivedEvent;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */

public class DebugMetricStore implements MetricStore {

    private Logger logger = Logger.getLogger(MetricStore.class);

    private EventBus bus;

    public DebugMetricStore(EventBus bus) {
        bus.register(this);
    }

    @Override
    @Subscribe
    @AllowConcurrentEvents
    public void store(Metric metric) {
        logger.debug("Storing metric: " + metric);
    }

    @Subscribe
    @AllowConcurrentEvents
    public void handle(MetricReceivedEvent metricReceivedEvent) {
        logger.debug("Storing metric: " + metricReceivedEvent.getMetric());
    }
}
