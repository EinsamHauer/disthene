package net.iponweb.disthene.service.store;

import com.datastax.driver.core.*;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.StoreErrorEvent;
import net.iponweb.disthene.events.StoreSuccessEvent;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * @author Andrei Ivanov
 */
public class BatchWriterThread extends WriterThread {
    private Logger logger = Logger.getLogger(BatchWriterThread.class);

    private int batchSize;
    private BatchStatement batch = new BatchStatement();

    private long lastFlushTimestamp = System.currentTimeMillis() / 1000L;

    public BatchWriterThread(String name, MBassador<DistheneEvent> bus, Session session, PreparedStatement statement, Queue<Metric> metrics, Executor executor, int batchSize) {
        super(name, bus, session, statement, metrics, executor);
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        while (!shutdown) {
            Metric metric = metrics.poll();
            if (metric != null) {
                addToBatch(metric);
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }

        if (batch.size() > 0) {
            flush();
        }
    }

    private void addToBatch(Metric metric) {
        batch.add(statement.bind(
                        metric.getRollup() * metric.getPeriod(),
                        Collections.singletonList(metric.getValue()),
                        metric.getTenant(),
                        metric.getRollup(),
                        metric.getPeriod(),
                        metric.getPath(),
                        metric.getTimestamp()
                )
        );

        //todo: interval via config?
        if (batch.size() >= batchSize || (lastFlushTimestamp < System.currentTimeMillis() / 1000L - 60)) {
            flush();
            lastFlushTimestamp = System.currentTimeMillis() / 1000L;
        }
    }

    private void flush() {
        final int batchSize = batch.size();
        ResultSetFuture future = session.executeAsync(batch);
        Futures.addCallback(future,
                new FutureCallback<ResultSet>() {
                    @Override
                    public void onSuccess(ResultSet result) {
                        bus.post(new StoreSuccessEvent(batchSize)).now();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        bus.post(new StoreErrorEvent(batchSize)).now();
                        logger.error(t);
                    }
                },
                executor
        );

        batch = new BatchStatement();
    }
}
