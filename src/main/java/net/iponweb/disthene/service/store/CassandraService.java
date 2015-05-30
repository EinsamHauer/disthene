package net.iponweb.disthene.service.store;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.google.common.util.concurrent.MoreExecutors;
import net.engio.mbassy.bus.MBassador;
import net.engio.mbassy.listener.Handler;
import net.engio.mbassy.listener.Listener;
import net.engio.mbassy.listener.References;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.StoreConfiguration;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.MetricStoreEvent;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author Andrei Ivanov
 */
@Listener(references = References.Strong)
public class CassandraService {
    private static final String QUERY = "UPDATE metric.metric USING TTL ? SET data = data + ? WHERE tenant = ? AND rollup = ? AND period = ? AND path = ? AND time = ?;";

    private Logger logger = Logger.getLogger(CassandraService.class);

    private Cluster cluster;
    private Session session;

    private Queue<Metric> metrics = new ConcurrentLinkedQueue<>();
    private List<WriterThread> writerThreads = new ArrayList<>();

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
                .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
                .withProtocolVersion(ProtocolVersion.V2)
                .withPort(storeConfiguration.getPort());
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
        PreparedStatement statement = session.prepare(QUERY);

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
        session.close();
        logger.info("Sleeping for 10 seconds to allow leftovers to be written");
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ignored) {
        }
        logger.info("Closing C* cluster");
        cluster.close();
    }
}


