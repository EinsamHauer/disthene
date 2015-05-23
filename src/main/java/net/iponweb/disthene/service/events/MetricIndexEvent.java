package net.iponweb.disthene.service.events;

import net.iponweb.disthene.bean.Metric;

/**
 * @author Andrei Ivanov
 */
public class MetricIndexEvent extends AbstractMetricEvent {

    public MetricIndexEvent(Metric metric) {
        super(metric);
    }
}
