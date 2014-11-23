package net.iponweb.disthene.service.store;

import net.iponweb.disthene.bean.Metric;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

/**
 * @author Andrei Ivanov
 */

@Service
public class MetricStoreImpl implements MetricStore {

    Logger logger = Logger.getLogger(MetricStore.class);


    @Override
    public void write(Metric metric) {
        logger.debug("Storing metric: " + metric);
    }
}
