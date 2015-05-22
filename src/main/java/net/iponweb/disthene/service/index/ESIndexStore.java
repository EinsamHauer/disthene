package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

/**
 * @author Andrei Ivanov
 */
public class ESIndexStore implements IndexStore {
    private Logger logger = Logger.getLogger(ESIndexStore.class);

    private BulkMetricProcessor processor;

    public ESIndexStore(DistheneConfiguration distheneConfiguration) throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", distheneConfiguration.getIndex().getName())
                .build();
        TransportClient client = new TransportClient(settings);
        for (String node : distheneConfiguration.getIndex().getCluster()) {
            client.addTransportAddress(new InetSocketTransportAddress(node, distheneConfiguration.getIndex().getPort()));
        }

        processor = new BulkMetricProcessor(client,
                distheneConfiguration.getIndex().getIndex(),
                distheneConfiguration.getIndex().getType(),
                distheneConfiguration.getIndex().getBulk().getActions(),
                TimeValue.timeValueSeconds(distheneConfiguration.getIndex().getBulk().getInterval()),
                distheneConfiguration.getIndex().getBulk().getSize()
        );
    }

    @Override
    public void store(final Metric metric) {
        processor.add(metric);
    }
}
