package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bus.DistheneBus;
import net.iponweb.disthene.bus.DistheneEventListener;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricStoreEvent;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Andrei Ivanov
 */
public class IndexService implements DistheneEventListener {
    private Logger logger = Logger.getLogger(IndexService.class);

    private BulkMetricProcessor processor;
    // tenant -> path -> dummy
    private ConcurrentMap<String, ConcurrentMap<String, Boolean>> cache = new ConcurrentHashMap<>();

    public IndexService(DistheneConfiguration distheneConfiguration, DistheneBus bus) {
        bus.subscribe(MetricStoreEvent.class, this);

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", distheneConfiguration.getIndex().getName())
                .build();
        TransportClient client = new TransportClient(settings);
        for (String node : distheneConfiguration.getIndex().getCluster()) {
            client.addTransportAddress(new InetSocketTransportAddress(node, distheneConfiguration.getIndex().getPort()));
        }

        processor = new BulkMetricProcessor(client, distheneConfiguration.getIndex());
    }

    @Override
    public void handle(DistheneEvent event) {
        if (event instanceof MetricStoreEvent) {
            ConcurrentMap<String, Boolean> tenantPaths = cache.get(((MetricStoreEvent) event).getMetric().getTenant());
            if (tenantPaths == null) {
                ConcurrentMap<String, Boolean> newTenantPaths = new ConcurrentHashMap<>();
                tenantPaths = cache.putIfAbsent(((MetricStoreEvent) event).getMetric().getTenant(), newTenantPaths);
                if (tenantPaths == null) {
                    tenantPaths = newTenantPaths;
                }
            }

            if (tenantPaths.putIfAbsent(((MetricStoreEvent) event).getMetric().getPath(), true) == null) {
                processor.add(((MetricStoreEvent) event).getMetric());
            }
        }
    }

    public void shutdown() {
        processor.shutdown();
    }

}
