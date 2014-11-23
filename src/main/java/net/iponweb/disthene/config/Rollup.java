package net.iponweb.disthene.config;

/**
 * @author Andrei Ivanov
 */
public class Rollup {

    private int rollup;
    private long period;

    public Rollup(String s) {
        String[] ss = s.split(":");
        rollup = Integer.valueOf(ss[0]);
        period = Long.valueOf(ss[1]);
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
}
