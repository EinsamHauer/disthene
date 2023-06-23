package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.CqlSession;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.events.DistheneEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrei Ivanov
 */
public abstract class WriterThread extends Thread {

    protected volatile boolean shutdown = false;

    protected final MBassador<DistheneEvent> bus;
    protected final CqlSession session;

    protected final TablesRegistry tablesRegistry;

    protected final BlockingQueue<Metric> metrics;

    protected final Executor executor;

    protected final AtomicInteger requestsInFlight = new AtomicInteger(0);

    public WriterThread(String name, MBassador<DistheneEvent> bus, CqlSession session, TablesRegistry tablesRegistry, BlockingQueue<Metric> metrics, Executor executor) {
        super(name);
        this.bus = bus;
        this.session = session;
        this.tablesRegistry = tablesRegistry;
        this.metrics = metrics;
        this.executor = executor;
    }

    public void shutdown() {
        shutdown = true;
        this.interrupt();
    }
}
