package net.iponweb.disthene.service.events;

/**
 * @author Andrei Ivanov
 */
public class StoreSuccessEvent {

    private int count;

    public StoreSuccessEvent(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }
}
