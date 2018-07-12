package net.iponweb.disthene.events;

import net.iponweb.disthene.bean.Metric;

public class MetricAggregateEvent extends AbstractMetricEvent {

    public MetricAggregateEvent(Metric metric) {
        super(metric);
    }
}
