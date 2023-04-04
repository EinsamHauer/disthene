package net.iponweb.disthene.config;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrei Ivanov
 */
@SuppressWarnings("unused")
public class StoreConfiguration {
    private List<String> cluster = new ArrayList<>();
    private String keyspace;
    private String columnFamily;
    private String userName;
    private String userPassword;
    private int port;
    private int maxConnections;
    private int readTimeout;
    private int connectTimeout;
    private int maxConcurrentRequests = 1024;
    private int maxQueueSize = 1024*1024;
    private boolean batch;
    private boolean topologyAware = false;
    private int batchSize;
    private int pool;

    public String getUserName() {
        return userName;
    }

    public String getUserPassword() {
        return userPassword;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setUserPassword(String userPassword) {
        this.userPassword = userPassword;
    }

    public List<String> getCluster() {
        return cluster;
    }

    public void setCluster(List<String> cluster) {
        this.cluster = cluster;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getMaxConcurrentRequests() {
        return maxConcurrentRequests;
    }

    public void setMaxConcurrentRequests(int maxConcurrentRequests) {
        this.maxConcurrentRequests = maxConcurrentRequests;
    }

    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
    }

    public boolean isTopologyAware() {
        return topologyAware;
    }

    public void setTopologyAware(boolean topologyAware) {
        this.topologyAware = topologyAware;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getPool() {
        return pool;
    }

    public void setPool(int pool) {
        this.pool = pool;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    @Override
    public String toString() {
        return "StoreConfiguration{" +
                "cluster=" + cluster +
                ", keyspace='" + keyspace + '\'' +
                ", columnFamily='" + columnFamily + '\'' +
                ", userName='" + userName + '\'' +
                ", userPassword='" + userPassword + '\'' +
                ", port=" + port +
                ", maxConnections=" + maxConnections +
                ", readTimeout=" + readTimeout +
                ", connectTimeout=" + connectTimeout +
                ", maxConcurrentRequests=" + maxConcurrentRequests +
                ", maxQueueSize=" + maxQueueSize +
                ", batch=" + batch +
                ", topologyAware=" + topologyAware +
                ", batchSize=" + batchSize +
                ", pool=" + pool +
                '}';
    }
}
