package net.iponweb.disthene.service.aggregate;

import net.iponweb.disthene.bean.Metric;

/**
 * @author Andrei Ivanov
 */
public interface Aggregator {

    void aggregate(Metric metric);
    void flush();
}
