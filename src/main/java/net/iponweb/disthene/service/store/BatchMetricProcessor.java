package net.iponweb.disthene.service.store;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.service.events.StoreErrorEvent;
import net.iponweb.disthene.service.events.StoreSuccessEvent;
import net.iponweb.disthene.service.stats.Stats;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Andrei Ivanov
 */
public class BatchMetricProcessor {
    private static final String QUERY = "UPDATE metric.metric USING TTL ? SET data = data + ? WHERE tenant = ? AND rollup = ? AND period = ? AND path = ? AND time = ?;";

    private Logger logger = Logger.getLogger(BatchMetricProcessor.class);

    private Session session;
    private PreparedStatement statement;
    private int batchSize;
    private Queue<Metric> metrics = new LinkedBlockingQueue<>();
    private AtomicBoolean executing = new AtomicBoolean(false);
    private MBassador bus;

    private final Executor executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    public BatchMetricProcessor(Session session, int batchSize, int flushInterval, MBassador bus) {
        this.session = session;
        this.batchSize = batchSize;
        this.bus = bus;

        statement = session.prepare(QUERY);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, flushInterval, flushInterval, TimeUnit.SECONDS);
    }


    public void add(Metric metric) {
        metrics.add(metric);
        executeIfNeeded();
    }

    private void executeIfNeeded() {
        if (batchSize != -1 && metrics.size() >= batchSize) {
            if (executing.compareAndSet(false, true)) {
                execute(batchSize);
            }
        }
    }

    private void flush() {
        if (metrics.size() > 0) {
            if (executing.compareAndSet(false, true)) {
                execute(batchSize);
            }
        }
    }

    private synchronized void execute(int minBatchSize) {
        while (metrics.size() >= minBatchSize) {
            int currentBatchSize = 0;
            final BatchStatement batch = new BatchStatement();

            while (currentBatchSize < batchSize && metrics.size() > 0) {
                Metric metric = metrics.remove();
                batch.add(
                    statement.bind(
                            metric.getRollup() * metric.getPeriod(),
                            Collections.singletonList(metric.getValue()),
                            metric.getTenant(),
                            metric.getRollup(),
                            metric.getPeriod(),
                            metric.getPath(),
                            metric.getUnixTimestamp()
                    )
                );
                currentBatchSize++;
            }

            ResultSetFuture future = session.executeAsync(batch);

            final int finalCurrentBatchSize = currentBatchSize;
            Futures.addCallback(future,
                    new FutureCallback<ResultSet>() {
                        @Override
                        public void onSuccess(ResultSet result) {
                            bus.post(new StoreSuccessEvent(finalCurrentBatchSize)).asynchronously();
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            bus.post(new StoreErrorEvent(finalCurrentBatchSize)).asynchronously();
                            logger.error(t);
                        }
                    },
                    executor
            );
        }

        executing.set(false);
    }


}
