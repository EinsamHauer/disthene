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
    private int maxRequests;
    private boolean batch;
    private int batchSize;
    private int pool;
    private String tenantTableTemplate = "metric_%s_%d"; //%s - tenant, %d rollup
    private String tenantKeyspace = null;
    private String tenantTableCreateTemplate = "CREATE TABLE IF NOT EXISTS %s.%s (\n" +
            "  path text,\n" +
            "  time bigint,\n" +
            "  data list<double>,\n" +
            "  PRIMARY KEY ((path), time)\n" +
            ") WITH CLUSTERING ORDER BY (time ASC)\n" +
            "  AND bloom_filter_fp_chance = 0.01\n" +
            "  AND caching = {'keys': 'ALL'}\n" +
            "  AND compaction = {'min_threshold': '2', 'unchecked_tombstone_compaction': 'true', 'tombstone_compaction_interval': '86400', 'min_sstable_size': '104857600', 'tombstone_threshold': '0.1', 'bucket_low': '0.5', 'bucket_high': '1.5', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}\n" +
            "  AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
//            "  AND dclocal_read_repair_chance = 0.1\n" +
            "  AND default_time_to_live = 0\n" +
            "  AND gc_grace_seconds = 43200\n";// +
//            "  AND read_repair_chance = 0.1;\n";

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

    public int getMaxRequests() {
        return maxRequests;
    }

    public void setMaxRequests(int maxRequests) {
        this.maxRequests = maxRequests;
    }

    public boolean isBatch() {
        return batch;
    }

    public void setBatch(boolean batch) {
        this.batch = batch;
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

    public String getTenantTableTemplate() {
        return tenantTableTemplate;
    }

    public void setTenantTableTemplate(String tenantTableTemplate) {
        this.tenantTableTemplate = tenantTableTemplate;
    }

    public String getTenantKeyspace() {
        return tenantKeyspace != null ? tenantKeyspace : keyspace;
    }

    public void setTenantKeyspace(String tenantKeyspace) {
        this.tenantKeyspace = tenantKeyspace;
    }

    public String getTenantTableCreateTemplate() {
        return tenantTableCreateTemplate;
    }

    public void setTenantTableCreateTemplate(String tenantTableCreateTemplate) {
        this.tenantTableCreateTemplate = tenantTableCreateTemplate;
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
                ", maxRequests=" + maxRequests +
                ", batch=" + batch +
                ", batchSize=" + batchSize +
                ", pool=" + pool +
                ", tenantTableTemplate='" + tenantTableTemplate + '\'' +
                ", tenantKeyspace='" + tenantKeyspace + '\'' +
                ", tenantTableCreateTemplate='" + tenantTableCreateTemplate + '\'' +
                '}';
    }
}
