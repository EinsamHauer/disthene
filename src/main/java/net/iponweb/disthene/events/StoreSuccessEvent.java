package net.iponweb.disthene.events;

/**
 * @author Andrei Ivanov
 */
public class StoreSuccessEvent implements DistheneEvent{

    private final int count;

    public StoreSuccessEvent(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }
}
