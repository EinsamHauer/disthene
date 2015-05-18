package net.iponweb.disthene.service.aggregate;

import net.iponweb.disthene.bean.AggregationRule;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.AggregationConfiguration;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.regex.Matcher;

/**
 * @author Andrei Ivanov
 */
public class BasicAggregator implements Aggregator {
    private Logger logger = Logger.getLogger(BasicAggregator.class);


    private DistheneConfiguration distheneConfiguration;
    private AggregationConfiguration aggregationConfiguration;

    public BasicAggregator(DistheneConfiguration distheneConfiguration, AggregationConfiguration aggregationConfiguration) {
        this.distheneConfiguration = distheneConfiguration;
        this.aggregationConfiguration = aggregationConfiguration;
    }

    @Override
    public void aggregate(Metric metric) {
        // Get aggregation rules
        List<AggregationRule> rules = aggregationConfiguration.getRules().get(metric.getTenant());

        for(AggregationRule rule : rules) {
            Matcher m = rule.getSource().matcher(metric.getPath());
            if (m.matches()) {
                // todo: handle names other than <data>
                logger.debug("Aggregating '" + metric.getPath() + "' to '" + rule.getDestination().replace("<data>", m.group("data")));
            }
        }
    }
}
