package net.iponweb.disthene.events;

import net.iponweb.disthene.bean.Metric;

/**
 * @author Andrei Ivanov
 */
public class MetricReceivedEvent extends AbstractMetricEvent {

    public MetricReceivedEvent(Metric metric) {
        super(metric);
    }
}
