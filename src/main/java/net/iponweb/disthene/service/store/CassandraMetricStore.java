package net.iponweb.disthene.service.store;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.DistheneConfiguration;
import net.iponweb.disthene.config.StoreConfiguration;
import net.iponweb.disthene.service.stats.Stats;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * @author Andrei Ivanov
 */

public class CassandraMetricStore implements MetricStore {

    private static final String QUERY = "UPDATE metric.metric USING TTL ? SET data = data + ? WHERE tenant = ? AND rollup = ? AND period = ? AND path = ? AND time = ?;";

    private Logger logger = Logger.getLogger(CassandraMetricStore.class);

    private Session session;
    private Executor executor;
    private Stats stats;
    private boolean batchMode;

    private BatchMetricProcessor processor;

    public CassandraMetricStore(StoreConfiguration storeConfiguration, Stats stats) {
        this.stats = stats;
        this.batchMode = storeConfiguration.isBatch();

        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

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
//                .withLoadBalancingPolicy(new WhiteListPolicy(new DCAwareRoundRobinPolicy(), Collections.singletonList(new InetSocketAddress("cassandra-1a.graphite.devops.iponweb.net", 9042))))
                .withPoolingOptions(poolingOptions)
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
                .withProtocolVersion(ProtocolVersion.V2)
                .withPort(storeConfiguration.getPort());
        for(String cp : storeConfiguration.getCluster()) {
            builder.addContactPoint(cp);
        }

        Cluster cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        logger.debug("Connected to cluster: " + metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            logger.debug(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(), host.getRack()));
        }

        session = cluster.connect();

        if (batchMode) {
            processor = new BatchMetricProcessor(session, storeConfiguration.getBatchSize(), storeConfiguration.getInterval(), stats);
        }
    }

    @Override
    public void store(Metric metric) {
        if (batchMode) {
            processor.add(metric);
        } else {
            storeInternal(metric);
        }
    }

    private void storeInternal(Metric metric) {
        stats.incMetricsWritten(metric);

        ResultSetFuture future = session.executeAsync(QUERY,
                metric.getRollup() * metric.getPeriod(),
                Collections.singletonList(metric.getValue()),
                metric.getTenant(),
                metric.getRollup(),
                metric.getPeriod(),
                metric.getPath(),
                metric.getUnixTimestamp()
        );

        Futures.addCallback(future,
                new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet result) {
                        stats.incStoreSuccess();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        stats.incStoreError();
                        logger.error(t);
                    }
                },
                executor
        );
    }
}


