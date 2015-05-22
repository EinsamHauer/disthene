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

    //todo: move all constants to config
    public CassandraMetricStore(DistheneConfiguration distheneConfiguration, Stats stats) {
        this.stats = stats;

        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReceiveBufferSize(8388608);
        socketOptions.setSendBufferSize(1048576);
        socketOptions.setTcpNoDelay(false);
        socketOptions.setReadTimeoutMillis(1000000);
        socketOptions.setReadTimeoutMillis(1000000);

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 8192);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 8192);
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, 128);
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 128);

        Cluster.Builder builder = Cluster.builder()
                .withSocketOptions(socketOptions)
                .withCompression(ProtocolOptions.Compression.LZ4)
//                .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
                .withLoadBalancingPolicy(new WhiteListPolicy(new DCAwareRoundRobinPolicy(), Collections.singletonList(new InetSocketAddress("cassandra-1a.graphite.devops.iponweb.net", 9042))))
                .withPoolingOptions(poolingOptions)
                .withProtocolVersion(ProtocolVersion.V2)
                .withPort(9042);
        for(String cp : distheneConfiguration.getStore().getCluster()) {
            builder.addContactPoint(cp);
        }
        Cluster cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        logger.debug("Connected to cluster: " + metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            logger.debug(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(), host.getRack()));
        }

        session = cluster.connect();

    }

    @Override
    public void store(Metric metric) {
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


