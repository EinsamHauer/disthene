package net.iponweb.disthene.bean;

import net.iponweb.disthene.config.Rollup;
import org.joda.time.DateTime;

/**
 * @author Andrei Ivanov
 */
public class Metric {

    private String tenant;
    private String path;
    private int rollup;
    private long period;
    private double value;
    private DateTime timestamp;

    public Metric(String input, Rollup rollup) {
        String[] splitInput = input.split("\\s");
        this.path = splitInput[0];
        this.value = Double.valueOf(splitInput[1]);
        this.timestamp = new DateTime(Long.valueOf(splitInput[2]));
        this.tenant = splitInput[3];
        this.rollup = rollup.getRollup();
        this.period = rollup.getPeriod();
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public int getRollup() {
        return rollup;
    }

    public void setRollup(int rollup) {
        this.rollup = rollup;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public DateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "{" + "tenant : " + tenant + ", path : " + path + ", value : " + value + ", time : " + timestamp.getMillis() * 1000 + ", rollup : " + rollup + ", period : " + period + "}";
    }
}
