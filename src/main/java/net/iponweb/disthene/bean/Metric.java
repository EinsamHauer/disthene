package net.iponweb.disthene.bean;

import net.iponweb.disthene.config.Rollup;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * @author Andrei Ivanov
 */
public class Metric {

    private MetricKey key;
    private double value;

    public Metric(String input, Rollup rollup) {
        String[] splitInput = input.split("\\s");
        this.key = new MetricKey(splitInput[3], splitInput[0], rollup.getRollup(), rollup.getPeriod(), new DateTime(Long.valueOf(splitInput[2]) * 1000L, DateTimeZone.UTC));
        this.value = Double.valueOf(splitInput[1]);
    }

    public Metric(String tenant, String path, int rollup, int period, double value, DateTime timestamp) {
        this.key = new MetricKey(tenant, path, rollup, period, timestamp);
        this.value = value;
    }

    public Metric(MetricKey key, double value) {
        this.key = key;
        this.value = value;
    }

    public String getId() {
        return getTenant() + "_" + getPath();
    }

    public String getTenant() {
        return key.getTenant();
    }

    public String getPath() {
        return key.getPath();
    }

    public int getRollup() {
        return key.getRollup();
    }

    public int getPeriod() {
        return key.getPeriod();
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public DateTime getTimestamp() {
        return key.getTimestamp();
    }

    public long getUnixTimestamp() {
        return key.getTimestamp().getMillis() / 1000;
    }

    @Override
    public String toString() {
        return "Metric{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
