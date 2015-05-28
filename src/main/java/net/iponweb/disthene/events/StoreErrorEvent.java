package net.iponweb.disthene.events;

/**
 * @author Andrei Ivanov
 */
public class StoreErrorEvent implements DistheneEvent {

    private int count;

    public StoreErrorEvent(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }
}
