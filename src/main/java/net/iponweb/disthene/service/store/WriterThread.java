package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.CqlSession;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.events.DistheneEvent;

import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * @author Andrei Ivanov
 */
public abstract class WriterThread extends Thread {

    protected volatile boolean shutdown = false;

    protected MBassador<DistheneEvent> bus;
    protected CqlSession session;

    protected TablesRegistry tablesRegistry;

    protected Queue<Metric> metrics;

    protected Executor executor;

    public WriterThread(String name, MBassador<DistheneEvent> bus, CqlSession session, TablesRegistry tablesRegistry, Queue<Metric> metrics, Executor executor) {
        super(name);
        this.bus = bus;
        this.session = session;
        this.tablesRegistry = tablesRegistry;
        this.metrics = metrics;
        this.executor = executor;
    }

    public void shutdown() {
        shutdown = true;
    }
}
