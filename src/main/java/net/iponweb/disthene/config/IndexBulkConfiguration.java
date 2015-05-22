package net.iponweb.disthene.config;

/**
 * @author Andrei Ivanov
 */
public class IndexBulkConfiguration {

    private int actions;
    private int interval;
    private int size;

    public int getActions() {
        return actions;
    }

    public void setActions(int actions) {
        this.actions = actions;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "IndexBulkConfiguration{" +
                "actions=" + actions +
                ", interval=" + interval +
                ", size=" + size +
                '}';
    }
}
