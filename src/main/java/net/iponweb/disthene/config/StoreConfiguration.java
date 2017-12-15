package net.iponweb.disthene.config;

import net.iponweb.disthene.util.CassandraLoadBalancingPolicies;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrei Ivanov
 */
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
    private String loadBalancingPolicyName = CassandraLoadBalancingPolicies.tokenDcAwareRoundRobinPolicy;
    private String protocolVersion = "V2";

    // tables properties
    private double fpChance = 0.01;
    private double dclocalReadRepairChance = 0.1;
    private long gcGraceSeconds = 86400;
    private double readRepairChance = 0.1;
    private long memtableFlushPeriodInMs = 60000;
    private int compactionMinThreshold = 2;
    private boolean uncheckedTombstoneCompaction = false;
    private double tombstoneThreshold = 0.1;
    private String compactionClass = "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy";
    private String compression = "org.apache.cassandra.io.compress.LZ4Compressor";


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

    public String getLoadBalancingPolicyName() {
        return loadBalancingPolicyName;
    }

    public void setLoadBalancingPolicyName(String policy) {
        this.loadBalancingPolicyName = policy;
    }

    public String getProtocolVersion() {
        return protocolVersion;
    }

    public double getFpChance() {
        return fpChance;
    }

    public void setFpChance(double fpChance) {
        this.fpChance = fpChance;
    }

    public double getDclocalReadRepairChance() {
        return dclocalReadRepairChance;
    }

    public void setDclocalReadRepairChance(double dclocalReadRepairChance) {
        this.dclocalReadRepairChance = dclocalReadRepairChance;
    }

    public long getGcGraceSeconds() {
        return gcGraceSeconds;
    }

    public void setGcGraceSeconds(long gcGraceSeconds) {
        this.gcGraceSeconds = gcGraceSeconds;
    }

    public double getReadRepairChance() {
        return readRepairChance;
    }

    public void setReadRepairChance(double readRepairChance) {
        this.readRepairChance = readRepairChance;
    }

    public long getMemtableFlushPeriodInMs() {
        return memtableFlushPeriodInMs;
    }

    public void setMemtableFlushPeriodInMs(long memtableFlushPeriodInMs) {
        this.memtableFlushPeriodInMs = memtableFlushPeriodInMs;
    }

    public int getCompactionMinThreshold() {
        return compactionMinThreshold;
    }

    public void setCompactionMinThreshold(int compactionMinThreshold) {
        this.compactionMinThreshold = compactionMinThreshold;
    }

    public boolean isUncheckedTombstoneCompaction() {
        return uncheckedTombstoneCompaction;
    }

    public void setUncheckedTombstoneCompaction(boolean uncheckedTombstoneCompaction) {
        this.uncheckedTombstoneCompaction = uncheckedTombstoneCompaction;
    }

    public double getTombstoneThreshold() {
        return tombstoneThreshold;
    }

    public void setTombstoneThreshold(double tombstoneThreshold) {
        this.tombstoneThreshold = tombstoneThreshold;
    }

    public String getCompactionClass() {
        return compactionClass;
    }

    public void setCompactionClass(String compactionClass) {
        this.compactionClass = compactionClass;
    }

    public String getCompression() {
        return compression;
    }

    public void setCompression(String compression) {
        this.compression = compression;
    }

    public void setProtocolVersion(String protocolVersion) {
        this.protocolVersion = protocolVersion;
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
                ", loadBalancingPolicyName='" + loadBalancingPolicyName + '\'' +
                ", protocolVersion='" + protocolVersion + '\'' +
                ", fpChance=" + fpChance +
                ", dclocalReadRepairChance=" + dclocalReadRepairChance +
                ", gcGraceSeconds=" + gcGraceSeconds +
                ", readRepairChance=" + readRepairChance +
                ", memtableFlushPeriodInMs=" + memtableFlushPeriodInMs +
                ", compactionMinThreshold=" + compactionMinThreshold +
                ", uncheckedTombstoneCompaction=" + uncheckedTombstoneCompaction +
                ", tombstoneThreshold=" + tombstoneThreshold +
                ", compactionClass='" + compactionClass + '\'' +
                ", compression='" + compression + '\'' +
                '}';
    }
}
