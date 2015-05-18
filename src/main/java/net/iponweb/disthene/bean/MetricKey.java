package net.iponweb.disthene.bean;

import org.joda.time.DateTime;

/**
 * @author Andrei Ivanov
 */
public class MetricKey {
    private String tenant;
    private String path;
    private int rollup;
    private long period;
    private DateTime timestamp;

    public MetricKey(String tenant, String path, int rollup, long period, DateTime timestamp) {
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

    public long getPeriod() {
        return period;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MetricKey)) return false;

        MetricKey metricKey = (MetricKey) o;

        if (period != metricKey.period) return false;
        if (rollup != metricKey.rollup) return false;
        if (!path.equals(metricKey.path)) return false;
        if (!tenant.equals(metricKey.tenant)) return false;
        if (!timestamp.equals(metricKey.timestamp)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tenant.hashCode();
        result = 31 * result + path.hashCode();
        result = 31 * result + rollup;
        result = 31 * result + (int) (period ^ (period >>> 32));
        result = 31 * result + timestamp.hashCode();
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
