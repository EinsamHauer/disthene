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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

/**
 * @author Andrei Ivanov
 */
public class IndexThread extends Thread {
    private Logger logger = Logger.getLogger(IndexThread.class);

    protected volatile boolean shutdown = false;

    private TransportClient client;
    protected Queue<Metric> metrics;
    private String index;
    private String type;
    private int batchSize;
    private int flushInterval;
    private long lastFlushTimestamp = System.currentTimeMillis() / 1000L;

    private MetricMultiGetRequestBuilder request;
    private BulkProcessor bulkProcessor;

    public IndexThread(String name, TransportClient client, Queue<Metric> metrics, String index, String type, int batchSize, int flushInterval) {
        super(name);
        this.client = client;
        this.metrics = metrics;
        this.index = index;
        this.type = type;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;

        request = new MetricMultiGetRequestBuilder(client, index, type);

        bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        logger.debug("stored " + request.numberOfActions() + " metrics");
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error(failure);
                    }
                })
                .setBulkActions(batchSize)
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                .setConcurrentRequests(1)
                .build();
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Metric metric = metrics.poll();
                if (metric != null) {
                    addToBatch(metric);
                } else {
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                logger.error("Encountered error in busy loop: ", e);
            }
        }

        if (request.size() > 0) {
            flush();
        }
    }

    private void addToBatch(Metric metric) {
        request.add(metric);

        if (request.size() >= batchSize || (lastFlushTimestamp < System.currentTimeMillis() / 1000L - flushInterval)) {
            flush();
            lastFlushTimestamp = System.currentTimeMillis() / 1000L;
        }
    }

    private void flush() {
        MultiGetResponse multiGetItemResponse = request.execute().actionGet();

        for(MultiGetItemResponse response : multiGetItemResponse.getResponses()) {
            if (response.isFailed()) {
                logger.error("Get failed: " + response.getFailure().getMessage());
            }

            Metric metric = request.metrics.get(response.getId());
            if (response.isFailed() || !response.getResponse().isExists()) {
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

        request = new MetricMultiGetRequestBuilder(client, index, type);
    }

    public void shutdown() {
        shutdown = true;
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

        public int size() {
            return  metrics.size();
        }
    }
}
