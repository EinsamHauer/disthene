package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.core.metadata.Node;
import net.engio.mbassy.bus.MBassador;
import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.events.DistheneEvent;
import net.iponweb.disthene.events.StoreErrorEvent;
import net.iponweb.disthene.events.StoreSuccessEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
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

    private final List<BoundStatement> statements = new LinkedList<>();

    private long lastFlushTimestamp = System.currentTimeMillis();

    BatchWriterThread(String name, MBassador<DistheneEvent> bus, CqlSession session, TablesRegistry tablesRegistry, BlockingQueue<Metric> metrics, Executor executor, int batchSize) {
        super(name, bus, session, tablesRegistry, metrics, executor);
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
            logger.error("Thread interrupted", e);
            this.interrupt();
        }
    }

    private void addToBatch(Metric metric) {
        PreparedStatement statement = tablesRegistry.getStatement(metric.getTenant(), metric.getRollup());
        if (statement == null) {
            logger.error("Unable to store metric " + metric + ". Can't get the statement");
            return;
        }

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

    private void flush() {
        List<List<BatchableStatement<?>>> batches = splitByToken();

        for (List<BatchableStatement<?>> batchStatements : batches) {
            BatchStatement batch = BatchStatement.newInstance(BatchType.UNLOGGED, batchStatements);
            final int batchSize = batchStatements.size();

            session
                    .executeAsync(batch)
                    .whenComplete((version, error) -> {
                        if (error != null) {
                            bus.post(new StoreErrorEvent(batchSize)).now();
                            logger.error(error);
                        } else {
                            bus.post(new StoreSuccessEvent(batchSize)).now();
                        }
                    });
        }

        statements.clear();
    }

    private List<List<BatchableStatement<?>>> splitByToken() {
        Map<Optional<Node>, List<BatchableStatement<?>>> batches = new HashMap<>();
        for (BoundStatement statement : statements) {
            Queue<Node> nodes = session.getContext().getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME).newQueryPlan(statement, session);

            // We are using TokenAwarePolicy without shuffling. Let's group by primary replica only then
            Optional<Node> primaryNode = nodes.isEmpty() ? Optional.empty() : Optional.of(nodes.poll());

            batches.computeIfAbsent(primaryNode, node -> new ArrayList<>()).add(statement);
        }
        return new ArrayList<>(batches.values());
    }

}
