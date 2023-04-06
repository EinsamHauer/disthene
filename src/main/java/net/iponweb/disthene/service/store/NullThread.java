package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.StoreErrorEvent;
import net.iponweb.disthene.events.StoreSuccessEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * @author Andrei Ivanov
 */
public class NullWriterThread extends WriterThread {
    private static final Logger logger = LogManager.getLogger(SingleWriterThread.class);

    public SingleWriterThread(String name, MBassador<DistheneEvent> bus, CqlSession session, String query, BlockingQueue<Metric> metrics, Executor executor) {
        super(name, bus, session, query, metrics, executor);
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                Metric metric = metrics.take();
                store(metric);
            }
        } catch (InterruptedException e) {
            if (!shutdown) logger.error("Thread interrupted", e);
            this.interrupt();
        }
    }

    private void store(Metric metric) {
        bus.post(new StoreSuccessEvent(1)).now();
    }

    @Override
    public void shutdown() {
    }
}
