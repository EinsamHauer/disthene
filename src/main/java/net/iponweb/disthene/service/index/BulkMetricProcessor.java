package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrei Ivanov
 */
public class BulkMetricProcessor {

    private Logger logger = Logger.getLogger(BulkMetricProcessor.class);

    private TransportClient client;
    private String index;
    private String type;


    private Queue<Metric> metrics = new LinkedBlockingQueue<>();
    private int bulkActions = 1000;

    private final Executor executor = Executors.newFixedThreadPool(1);
    private BulkProcessor bulkProcessor;

    private AtomicLong writeCount = new AtomicLong(0);

    public BulkMetricProcessor(TransportClient client, String index, String type, int bulkActions, TimeValue flushInterval, int size) {
        this.client = client;
        this.index = index;
        this.type = type;
        this.bulkActions = bulkActions;

        if (flushInterval != null) {
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    flush();
                }
            }, flushInterval.getMillis(), flushInterval.getMillis(), TimeUnit.MILLISECONDS);
        }

        bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        writeCount.getAndAdd(response.getItems().length);
                        logger.debug("stored " + writeCount.get() + " metrics");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error(failure);
                    }
                })
                .setBulkActions(bulkActions)
                .setBulkSize(new ByteSizeValue(size, ByteSizeUnit.MB))
                .setFlushInterval(flushInterval)
                .setConcurrentRequests(1)
                .build();
    }

    public synchronized void add(Metric metric) {
        metrics.add(metric);
        executeIfNeeded();
    }

    private void executeIfNeeded() {
        if (bulkActions != -1 && metrics.size() >= bulkActions) {
            execute();
        }
    }

    private synchronized void flush() {
        if (metrics.size() > 0) {
            execute();
        }
    }

    private void execute() {
        // What we do here:
        // - pop bulkActions items from the metrics list
        // - run MultiGet request inside executor with callbacks

        while (metrics.size() >= bulkActions) {
            int currentBatchSize = 0;
            MetricMultiGetRequestBuilder request = new MetricMultiGetRequestBuilder(client, index, type);

            while (currentBatchSize < bulkActions) {
                request.add(metrics.remove());
                currentBatchSize++;
            }

            executor.execute(new MetricMultiGetProcessor(request));

        }
    }

    private class MetricMultiGetRequestBuilder extends MultiGetRequestBuilder {

        private String index;
        private String type;
        Map<String, Metric> metrics = new HashMap<>();


        public MetricMultiGetRequestBuilder(Client client, String index, String type) {
            super(client);
            this.index = index;
            this.type = type;
        }

        public MultiGetRequestBuilder add(Metric metric) {
            metrics.put(metric.getId(), metric);
            return super.add(index, type, metric.getId());
        }
    }

    private class MetricMultiGetProcessor implements Runnable {

        MetricMultiGetRequestBuilder request;

        private MetricMultiGetProcessor(MetricMultiGetRequestBuilder request) {
            this.request = request;
        }

        @Override
        public void run() {
            MultiGetResponse multiGetItemResponse = request.execute().actionGet();

            for(MultiGetItemResponse response : multiGetItemResponse.getResponses()) {
                Metric metric = request.metrics.get(response.getId());
                if (!response.getResponse().isExists()) {
//                            logger.debug("Metric " + metric.getPath() + " exists not");
                    // add bulk request here

                    final String[] parts = metric.getPath().split("\\.");
                    final StringBuilder sb = new StringBuilder();

                    for (int i = 0; i < parts.length; i++) {
                        if (sb.toString().length() > 0) {
                            sb.append(".");
                        }
                        sb.append(parts[i]);
                        try {
                            bulkProcessor.add(new IndexRequest(index, type, metric.getTenant() + "_" + sb.toString()).source(
                                    XContentFactory.jsonBuilder().startObject()
                                            .field("tenant", metric.getTenant())
                                            .field("path", sb.toString())
                                            .field("depth", (i + 1))
                                            .field("leaf", (i == parts.length - 1))
                                            .endObject()
                            ));
                        } catch (IOException e) {
                            logger.error(e);
                        }

                    }
                }
            }
        }
    }

}
