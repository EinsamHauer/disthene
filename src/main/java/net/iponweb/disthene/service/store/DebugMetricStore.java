package net.iponweb.disthene.service.store;

import net.iponweb.disthene.bean.Metric;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */

public class DebugMetricStore implements MetricStore {

    private Logger logger = Logger.getLogger(MetricStore.class);


    @Override
    public void write(Metric metric) {
        logger.debug("Storing metric: " + metric);
    }
}
