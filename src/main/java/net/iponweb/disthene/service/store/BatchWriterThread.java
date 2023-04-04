package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.StoreErrorEvent;
import net.iponweb.disthene.events.StoreSuccessEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;

/**
 * @author Andrei Ivanov
 */
class BatchWriterThread extends WriterThread {
    //todo: interval via config?
    private static final long INTERVAL = 60_000;

    private static final Logger logger = LogManager.getLogger(BatchWriterThread.class);

    private final int batchSize;

    private final List<BatchableStatement<?>> statements = new LinkedList<>();

    private long lastFlushTimestamp = System.currentTimeMillis();

    BatchWriterThread(String name, MBassador<DistheneEvent> bus, CqlSession session, String query, BlockingQueue<Metric> metrics, Executor executor, int batchSize) {
        super(name, bus, session, query, metrics, executor);
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        try {
            while (!shutdown) {
                Metric metric = metrics.take();
                addToBatch(metric);
            }

            if (statements.size() > 0) {
                flush();
            }
        } catch (InterruptedException e) {
            if (!shutdown) logger.error("Thread interrupted", e);
            this.interrupt();
        }
    }

    private void addToBatch(Metric metric) {
        PreparedStatement statement = session.prepare(query);
        statements.add(
                statement.bind(
                        metric.getRollup() * metric.getPeriod(),
                        Collections.singletonList(metric.getValue()),
                        metric.getPath(),
                        metric.getTimestamp()
                )
        );

        if (statements.size() >= batchSize || (lastFlushTimestamp < System.currentTimeMillis() - INTERVAL)) {
            lastFlushTimestamp = System.currentTimeMillis();
            flush();
        }
    }

    private synchronized void flush() {
        BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED, statements);

        requestsInFlight.incrementAndGet();
        session
                .executeAsync(batch)
                .whenComplete((version, error) -> {
                    requestsInFlight.decrementAndGet();
                    if (error != null) {
                        bus.post(new StoreErrorEvent(statements.size())).now();
                        logger.error(error);
                    } else {
                        bus.post(new StoreSuccessEvent(statements.size())).now();
                    }
                });

        statements.clear();
    }

    @Override
    public void shutdown() {
        shutdown = true;

        logger.info("Flushing leftovers");
        flush();

        while (requestsInFlight.get() > 0) {
            logger.info("Requests in flight: " + requestsInFlight.get());
            try {
                //noinspection BusyWait
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Wait interrupted", e);
            }
        }

        this.interrupt();
    }
}
