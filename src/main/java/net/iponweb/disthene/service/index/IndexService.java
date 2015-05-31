package net.iponweb.disthene.service.index;

import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.IndexConfiguration;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricStoreEvent;
import org.apache.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Andrei Ivanov
 */
@Listener(references= References.Strong)
public class IndexService {
    private Logger logger = Logger.getLogger(IndexService.class);

    TransportClient client;
    private IndexThread indexThread;

    // tenant -> path -> dummy
    private ConcurrentMap<String, ConcurrentMap<String, Boolean>> cache = new ConcurrentHashMap<>();
    private Queue<Metric> metrics = new ConcurrentLinkedQueue<>();

    public IndexService(IndexConfiguration indexConfiguration, MBassador<DistheneEvent> bus) {
        bus.subscribe(this);

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", indexConfiguration.getName())
                .build();
        client = new TransportClient(settings);
        for (String node : indexConfiguration.getCluster()) {
            client.addTransportAddress(new InetSocketTransportAddress(node, indexConfiguration.getPort()));
        }

        indexThread = new IndexThread(
                "distheneIndexThread",
                client,
                metrics,
                indexConfiguration.getIndex(),
                indexConfiguration.getType(),
                indexConfiguration.getBulk().getActions(),
                indexConfiguration.getBulk().getInterval()
        );

        indexThread.start();
    }

    private ConcurrentMap<String, Boolean> getTenantPaths(String tenant) {
        ConcurrentMap<String, Boolean> tenantPaths = cache.get(tenant);
        if (tenantPaths == null) {
            ConcurrentMap<String, Boolean> newTenantPaths = new ConcurrentHashMap<>();
            tenantPaths = cache.putIfAbsent(tenant, newTenantPaths);
            if (tenantPaths == null) {
                tenantPaths = newTenantPaths;
            }
        }

        return tenantPaths;
    }

    @Handler(rejectSubtypes = false)
    public void handle(MetricStoreEvent metricStoreEvent) {
        if (getTenantPaths(metricStoreEvent.getMetric().getTenant()).putIfAbsent(metricStoreEvent.getMetric().getPath(), true) == null) {
            metrics.offer(metricStoreEvent.getMetric());
        }
    }

    public void shutdown() {
        indexThread.shutdown();
        logger.info("Closing ES client");
        client.close();
    }
}
