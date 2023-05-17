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
    private Logger logger = Logger.getLogger(CassandraService.class);

    private Cluster cluster;
    private Session session;

    private Queue<Metric> metrics = new ConcurrentLinkedQueue<>();
    private List<WriterThread> writerThreads = new ArrayList<>();

    public CassandraService(StoreConfiguration storeConfiguration, MBassador<DistheneEvent> bus) {
        bus.subscribe(this);

        String query = "UPDATE " +
                storeConfiguration.getKeyspace() + "." + storeConfiguration.getColumnFamily() +
                " USING TTL ? SET data = data + ? WHERE tenant = ? AND rollup = ? AND period = ? AND path = ? AND time = ?;";

        SocketOptions socketOptions = new SocketOptions()
                .setReceiveBufferSize(1024 * 1024)
                .setSendBufferSize(1024 * 1024)
                .setTcpNoDelay(false)
                .setReadTimeoutMillis(storeConfiguration.getReadTimeout() * 1000)
                .setConnectTimeoutMillis(storeConfiguration.getConnectTimeout() * 1000);

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, storeConfiguration.getMaxConnections());
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, storeConfiguration.getMaxConnections());
        poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, storeConfiguration.getMaxRequests());
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, storeConfiguration.getMaxRequests());

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
        PreparedStatement statement = session.prepare(query);

        // Creating writers

        if (storeConfiguration.isBatch()) {
            for (int i = 0; i < storeConfiguration.getPool(); i++) {
                WriterThread writerThread = new BatchWriterThread(
                        "distheneCassandraBatchWriter" + i,
                        bus,
                        session,
                        statement,
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
                        statement,
                        metrics,
                        MoreExecutors.listeningDecorator(Executors.newCachedThreadPool())
                );

                writerThreads.add(writerThread);
                writerThread.start();
            }
        }
    }

    @Handler(rejectSubtypes = false)
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


