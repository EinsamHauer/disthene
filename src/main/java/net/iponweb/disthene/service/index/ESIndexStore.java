package net.iponweb.disthene.service.index;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.service.events.MetricIndexEvent;
import net.iponweb.disthene.service.events.MetricReceivedEvent;
import net.iponweb.disthene.service.events.MetricStoreEvent;
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

    public ESIndexStore(DistheneConfiguration distheneConfiguration, MBassador bus) {
        bus.subscribe(this);

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", distheneConfiguration.getIndex().getName())
                .build();
        TransportClient client = new TransportClient(settings);
        for (String node : distheneConfiguration.getIndex().getCluster()) {
            client.addTransportAddress(new InetSocketTransportAddress(node, distheneConfiguration.getIndex().getPort()));
        }

        processor = new BulkMetricProcessor(client, distheneConfiguration.getIndex());
    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricIndexEvent metricIndexEvent) {
        processor.add(metricIndexEvent.getMetric());
    }

    @Override
    public void store(final Metric metric) {
    }
}
