package net.iponweb.disthene.service.events;

/**
 * @author Andrei Ivanov
 */
public class StoreSuccessEvent implements DistheneEvent{

    private int count;

    public StoreSuccessEvent(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }
}
