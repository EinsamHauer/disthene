package net.iponweb.disthene.service.events;

/**
 * @author Andrei Ivanov
 */
public class StoreErrorEvent {

    private int count;

    public StoreErrorEvent(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }
}
