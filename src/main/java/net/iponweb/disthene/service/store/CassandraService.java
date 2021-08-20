package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import com.google.common.util.concurrent.MoreExecutors;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.StoreConfiguration;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricStoreEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * @author Andrei Ivanov
 */
@Listener(references = References.Strong)
public class CassandraService {
    private static final Logger logger = LogManager.getLogger(CassandraService.class);

    private final CqlSession session;

    private final Queue<Metric> metrics = new ConcurrentLinkedQueue<>();
    private final List<WriterThread> writerThreads = new ArrayList<>();

    public CassandraService(StoreConfiguration storeConfiguration, MBassador<DistheneEvent> bus) {
        bus.subscribe(this);

        DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withStringList(DefaultDriverOption.CONTACT_POINTS, getContactPoints(storeConfiguration))
                        .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, storeConfiguration.getMaxRequests())
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(storeConfiguration.getReadTimeout()))
                        .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_ONE")
                        .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DcInferringLoadBalancingPolicy.class)
                .build();

        session = CqlSession.builder()
                .withConfigLoader(loader)
                .build();

        Metadata metadata = session.getMetadata();
        logger.debug("Connected to cluster: " + metadata.getClusterName());
        for (Node node : metadata.getNodes().values()) {
            logger.debug(String.format("Datacenter: %s; Host: %s; Rack: %s",
                    node.getDatacenter(),
                    node.getBroadcastAddress().isPresent() ? node.getBroadcastAddress().get().toString() : "unknown", node.getRack()));
        }

        TablesRegistry tablesRegistry = new TablesRegistry(session, storeConfiguration);

        // Creating writers
        if (storeConfiguration.isBatch()) {
            for (int i = 0; i < storeConfiguration.getPool(); i++) {
                WriterThread writerThread = new BatchWriterThread(
                        "distheneCassandraBatchWriter" + i,
                        bus,
                        session,
                        tablesRegistry,
                        metrics,
                        MoreExecutors.listeningDecorator(Executors.newCachedThreadPool()),
                        storeConfiguration.getBatchSize()
                );

                writerThreads.add(writerThread);
                writerThread.start();
            }
        } else {
            for (int i = 0; i < storeConfiguration.getPool(); i++) {
                WriterThread writerThread = new SingleWriterThread(
                        "distheneCassandraSingleWriter" + i,
                        bus,
                        session,
                        tablesRegistry,
                        metrics,
                        MoreExecutors.listeningDecorator(Executors.newCachedThreadPool())
                );

                writerThreads.add(writerThread);
                writerThread.start();
            }
        }
    }

    private List<String> getContactPoints(StoreConfiguration storeConfiguration) {
        return storeConfiguration.getCluster().stream().map(s -> s + ":" + storeConfiguration.getPort()).collect(Collectors.toList());
    }

    @SuppressWarnings("unused")
    @Handler
    public void handle(MetricStoreEvent metricStoreEvent) {
        metrics.offer(metricStoreEvent.getMetric());
    }

    public void shutdown() {
        for (WriterThread writerThread : writerThreads) {
            writerThread.shutdown();
        }

        logger.info("Closing C* session");
        session.close();
    }
}


