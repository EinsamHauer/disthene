package net.iponweb.disthene.bean;

import net.iponweb.disthene.config.Rollup;
import org.joda.time.DateTime;

/**
 * @author Andrei Ivanov
 */
public class Metric {

    private MetricKey key;
    private double value;

    public Metric(String input, Rollup rollup) {
        String[] splitInput = input.split("\\s");
        this.key = new MetricKey(splitInput[3], splitInput[0], rollup.getRollup(), rollup.getPeriod(), new DateTime(Long.valueOf(splitInput[2])));
        this.value = Double.valueOf(splitInput[1]);
    }

    public Metric(String tenant, String path, int rollup, long period, double value, DateTime timestamp) {
        this.key = new MetricKey(tenant, path, rollup, period, timestamp);
        this.value = value;
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

    public long getPeriod() {
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

    @Override
    public String toString() {
        return "Metric{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}
