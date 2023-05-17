package net.iponweb.disthene.bean;

/**
 * @author Andrei Ivanov
 */
public class MetricKey {
    private String tenant;
    private String path;
    private int rollup;
    private int period;
    private long timestamp;

    public MetricKey(String tenant, String path, int rollup, int period, long timestamp) {
        this.tenant = tenant;
        this.path = path;
        this.rollup = rollup;
        this.period = period;
        this.timestamp = timestamp;
    }

    public String getTenant() {
        return tenant;
    }

    public String getPath() {
        return path;
    }

    public int getRollup() {
        return rollup;
    }

    public int getPeriod() {
        return period;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricKey)) return false;

        MetricKey metricKey = (MetricKey) o;

        return period == metricKey.period && rollup == metricKey.rollup && timestamp == metricKey.timestamp && path.equals(metricKey.path) && tenant.equals(metricKey.tenant);

    }

    @Override
    public int hashCode() {
        int result = tenant.hashCode();
        result = 31 * result + path.hashCode();
        result = 31 * result + rollup;
        result = 31 * result + period;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "MetricKey{" +
                "tenant='" + tenant + '\'' +
                ", path='" + path + '\'' +
                ", rollup=" + rollup +
                ", period=" + period +
                ", timestamp=" + timestamp +
                '}';
    }
}
