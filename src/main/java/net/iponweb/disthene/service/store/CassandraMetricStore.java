package net.iponweb.disthene.service.store;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.log4j.Logger;

/**
 * @author Andrei Ivanov
 */

public class CassandraMetricStore implements MetricStore {

    private Logger logger = Logger.getLogger(CassandraMetricStore.class);

    private DistheneConfiguration distheneConfiguration;

    public CassandraMetricStore(DistheneConfiguration distheneConfiguration) {
        this.distheneConfiguration = distheneConfiguration;
    }

    @Override
    public void store(Metric metric) {
        logger.debug("Storing metric to C*: " + metric);
    }
}


