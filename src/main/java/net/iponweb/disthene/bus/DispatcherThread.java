package net.iponweb.disthene.bus;

import net.iponweb.disthene.events.DistheneEvent;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * @author Andrei Ivanov
 */
public class DispatcherThread extends Thread {

    protected volatile boolean shutdown = false;

    private Queue<DistheneEvent> eventQueue;
    private Map<Class, Set<DistheneEventListener>> listeners;

    public DispatcherThread(String name, Queue<DistheneEvent> eventQueue, Map<Class, Set<DistheneEventListener>> listeners) {
        super(name);
        this.eventQueue = eventQueue;
        this.listeners = listeners;
    }

    @Override
    public void run() {
        while (!shutdown) {
            DistheneEvent event = eventQueue.poll();
            if (event != null) {
                Set<DistheneEventListener> listenerSet = listeners.get(event.getClass());
                for (DistheneEventListener listener : listenerSet) {
                    listener.handle(event);
                }
            }
        }
    }


    public void shutdown() {
        shutdown = true;
    }
}
