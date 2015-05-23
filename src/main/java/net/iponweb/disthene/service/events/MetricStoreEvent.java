package net.iponweb.disthene.service.events;

import net.iponweb.disthene.bean.Metric;

/**
 * @author Andrei Ivanov
 */
public class MetricStoreEvent extends AbstractMetricEvent {
    public MetricStoreEvent(Metric metric) {
        super(metric);
    }
}
