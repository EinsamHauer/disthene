package net.iponweb.disthene.service.store;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.MoreExecutors;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.StoreConfiguration;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricStoreEvent;
import net.iponweb.disthene.util.CassandraLoadBalancingPolicies;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

/**
 * @author Andrei Ivanov
 */
@Listener(references = References.Strong)
public class CassandraService {
    private static final Logger logger = Logger.getLogger(CassandraService.class);

    private final Cluster cluster;
    private final Session session;

    private final Queue<Metric> metrics = new ConcurrentLinkedQueue<>();
    private final List<WriterThread> writerThreads = new ArrayList<>();

    public CassandraService(StoreConfiguration storeConfiguration, MBassador<DistheneEvent> bus) {
        bus.subscribe(this);

        SocketOptions socketOptions = new SocketOptions()
                .setReceiveBufferSize(1024 * 1024)
                .setSendBufferSize(1024 * 1024)
                .setTcpNoDelay(false)
                .setReadTimeoutMillis(storeConfiguration.getReadTimeout() * 1000)
                .setConnectTimeoutMillis(storeConfiguration.getConnectTimeout() * 1000);

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, storeConfiguration.getMaxConnections());
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, storeConfiguration.getMaxConnections());
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, storeConfiguration.getMaxRequests());
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, storeConfiguration.getMaxRequests());

        Cluster.Builder builder = Cluster.builder()
                .withSocketOptions(socketOptions)
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withLoadBalancingPolicy(CassandraLoadBalancingPolicies.getLoadBalancingPolicy(storeConfiguration.getLoadBalancingPolicyName()))
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
                .withProtocolVersion(ProtocolVersion.valueOf(storeConfiguration.getProtocolVersion()))
                .withPort(storeConfiguration.getPort());

        if ( storeConfiguration.getUserName() != null && storeConfiguration.getUserPassword() != null ) {
            builder = builder.withCredentials(storeConfiguration.getUserName(), storeConfiguration.getUserPassword());
        }

        for (String cp : storeConfiguration.getCluster()) {
            builder.addContactPoint(cp);
        }

        cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        logger.debug("Connected to cluster: " + metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            logger.debug(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(), host.getRack()));
        }

        session = cluster.connect();

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

    @SuppressWarnings("unused")
    @Handler()
    public void handle(MetricStoreEvent metricStoreEvent) {
        metrics.offer(metricStoreEvent.getMetric());
    }

    public void shutdown() {
        for (WriterThread writerThread : writerThreads) {
            writerThread.shutdown();
        }

        logger.info("Closing C* session");
        logger.info("Waiting for C* queries to be completed");
        while (getInFlightQueries(session.getState()) > 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ignored) {
            }
        }
        session.close();
        logger.info("Closing C* cluster");
        cluster.close();
    }

    private int getInFlightQueries(Session.State state) {
        int result = 0;
        Collection<Host> hosts = state.getConnectedHosts();
        for(Host host : hosts) {
            result += state.getInFlightQueries(host);
        }

        return result;
    }
}


