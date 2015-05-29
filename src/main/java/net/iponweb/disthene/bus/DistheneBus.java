package net.iponweb.disthene.bus;

import net.iponweb.disthene.events.DistheneEvent;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Andrei Ivanov
 */
public class DistheneBus {

    private Queue<DistheneEvent> eventQueue = new ConcurrentLinkedQueue<>();
    private Map<Class, Set<DistheneEventListener>>  listeners = new HashMap<>();
    private List<DispatcherThread> dispatcherThreads = new ArrayList<>();


    public DistheneBus() {

        for (int i = 0; i < 32; i++) {
            DispatcherThread thread = new DispatcherThread(
                    "distheneEventDispatcher" + i,
                    eventQueue,
                    listeners
            );
            dispatcherThreads.add(thread);
            thread.start();
        }

    }

    public void post(DistheneEvent event) {
        eventQueue.offer(event);
    }

    public void subscribe(Class eventClass, DistheneEventListener listener) {
        if (!DistheneEvent.class.isAssignableFrom(eventClass)) {
            return;
        }

        Set<DistheneEventListener> listenerSet = listeners.get(eventClass);
        if (listenerSet == null) {
            listenerSet = new HashSet<DistheneEventListener>();
            listeners.put(eventClass, listenerSet);
        }

        listenerSet.add(listener);

    }

    public void shutdown() {
        for (DispatcherThread thread : dispatcherThreads) {
            thread.shutdown();
        }

        //todo: process leftovers
    }
}
