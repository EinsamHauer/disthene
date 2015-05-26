package net.iponweb.disthene.service.index;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.service.events.DistheneEvent;
import net.iponweb.disthene.service.events.MetricStoreEvent;
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
@Listener(references= References.Strong)
public class ESIndexStore {
    private Logger logger = Logger.getLogger(ESIndexStore.class);

    private BulkMetricProcessor processor;
    // tenant -> path -> dummy
    private ConcurrentMap<String, ConcurrentMap<String, Boolean>> cache = new ConcurrentHashMap<>();

    public ESIndexStore(DistheneConfiguration distheneConfiguration, MBassador<DistheneEvent> bus) {
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
    public void handle(MetricStoreEvent metricStoreEvent) {
        ConcurrentMap<String, Boolean> tenantPaths = cache.get(metricStoreEvent.getMetric().getTenant());
        if (tenantPaths == null) {
            ConcurrentMap<String, Boolean> newTenantPaths = new ConcurrentHashMap<>();
            tenantPaths = cache.putIfAbsent(metricStoreEvent.getMetric().getTenant(), newTenantPaths);
            if (tenantPaths == null) {
                tenantPaths = newTenantPaths;
            }
        }

        if (tenantPaths.putIfAbsent(metricStoreEvent.getMetric().getPath(), true) == null) {
            processor.add(metricStoreEvent.getMetric());
        }

    }

    public void shutdown() {
        processor.shutdown();
    }
}
