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
import java.util.Queue;
import java.util.concurrent.Executor;

/**
 * @author Andrei Ivanov
 */
public class SingleWriterThread extends WriterThread {
    private static final Logger logger = LogManager.getLogger(SingleWriterThread.class);

    public SingleWriterThread(String name, MBassador<DistheneEvent> bus, CqlSession session, TablesRegistry tablesRegistry, Queue<Metric> metrics, Executor executor) {
        super(name, bus, session, tablesRegistry, metrics, executor);
    }

    @Override
    public void run() {
        while (!shutdown) {
            Metric metric = metrics.poll();
            if (metric != null) {
                store(metric);
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    private void store(Metric metric) {
        PreparedStatement statement = tablesRegistry.getStatement(metric.getTenant(), metric.getRollup());
        if (statement == null) {
            logger.error("Unable to store metric " + metric + ". Can't get the statement");
            return;
        }

        session.executeAsync(
                statement.bind(
                        metric.getRollup() * metric.getPeriod(),
                        Collections.singletonList(metric.getValue()),
                        metric.getPath(),
                        metric.getTimestamp()
                )).whenComplete((version, error) -> {
            if (error != null) {
                bus.post(new StoreErrorEvent(1)).now();
                logger.error(error);
            } else {
                bus.post(new StoreSuccessEvent(1)).now();
            }
        });
    }

}
