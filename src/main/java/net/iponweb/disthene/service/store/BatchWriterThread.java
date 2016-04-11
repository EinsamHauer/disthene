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

import java.util.*;
import java.util.concurrent.Executor;

/**
 * @author Andrei Ivanov
 */
class BatchWriterThread extends WriterThread {
    //todo: interval via config?
    private static final long INTERVAL = 60_000;

    private Logger logger = Logger.getLogger(BatchWriterThread.class);

    private int batchSize;

    private List<Statement> statements = new LinkedList<>();

    private long lastFlushTimestamp = System.currentTimeMillis();

    BatchWriterThread(String name, MBassador<DistheneEvent> bus, Session session, PreparedStatement statement, Queue<Metric> metrics, Executor executor, int batchSize) {
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

        if (statements.size() > 0) {
            flush();
        }
    }

    private void addToBatch(Metric metric) {
        statements.add(statement.bind(
                            metric.getRollup() * metric.getPeriod(),
                            Collections.singletonList(metric.getValue()),
                            metric.getTenant(),
                            metric.getRollup(),
                            metric.getPeriod(),
                            metric.getPath(),
                            metric.getTimestamp()
                )
        );

        if (statements.size() >= batchSize || (lastFlushTimestamp < System.currentTimeMillis() - INTERVAL)) {
            lastFlushTimestamp = System.currentTimeMillis();
            flush();
        }
    }

    private void flush() {
        List<List<Statement>> batches = splitByToken();

        for (List<Statement> batchStatements : batches) {
            BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            final int batchSize = batchStatements.size();

            for (Statement s : batchStatements) {
                batch.add(s);
            }

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
        }

        statements.clear();

    }

    private List<List<Statement>> splitByToken() {
        Map<Set<Host>,List<Statement>> batches = new HashMap<>();
        for (Statement statement : statements) {
            Set<Host> hosts = new HashSet<>();

            Iterator<Host> it = session.getCluster().getConfiguration().getPolicies().
                    getLoadBalancingPolicy().newQueryPlan(statement.getKeyspace(), statement);

            // We are using TokenAwarePolicy without shuffling. Let's group by primary replica only then
            if (it.hasNext()) {
                hosts.add(it.next());
            }

            List<Statement> tokenBatch = batches.get(hosts);
            if (tokenBatch == null) {
                tokenBatch = new ArrayList<>();
                batches.put(hosts, tokenBatch);
            }
            tokenBatch.add(statement);
        }
        return new ArrayList<>(batches.values());
    }

}
