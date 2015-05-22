package net.iponweb.disthene.service.general;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.service.aggregate.Aggregator;
import net.iponweb.disthene.service.blacklist.BlackList;
import net.iponweb.disthene.service.index.IndexStore;
import net.iponweb.disthene.service.stats.Stats;
import net.iponweb.disthene.service.store.MetricStore;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Andrei Ivanov
 */
public class GeneralStore {

    private Logger logger = Logger.getLogger(GeneralStore.class);

    private MetricStore metricStore;
    private IndexStore indexStore;
    private BlackList blackList;
    private Aggregator aggregator;
    private Aggregator rollupAggregator;
    private Stats stats;

    public GeneralStore(MetricStore metricStore, IndexStore indexStore, BlackList blackList, Aggregator aggregator, Aggregator rollupAggregator, Stats stats) {
        this.metricStore = metricStore;
        this.indexStore = indexStore;
        this.blackList = blackList;
        this.aggregator = aggregator;
        this.rollupAggregator = rollupAggregator;
        this.stats = stats;
    }

    public void store(Metric metric) {
        // aggregate
        try {
            stats.incMetricsReceived(metric);
            aggregator.aggregate(metric);

            if (!blackList.isBlackListed(metric)) {
                indexStore.store(metric);
                metricStore.store(metric);
                rollupAggregator.aggregate(metric);
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }
}
