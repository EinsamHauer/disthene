package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * @author Andrei Ivanov
 */
public class IndexThread extends Thread {
    private static final Logger logger = LogManager.getLogger(IndexThread.class);

    protected volatile boolean shutdown = false;

    private final RestHighLevelClient client;
    protected final BlockingQueue<Metric> metrics;
    private final String index;
    private final int batchSize;
    private final int flushInterval;
    private long lastFlushTimestamp = System.currentTimeMillis() / 1000L;

    private MetricMultiGetRequest request;
    private final BulkProcessor bulkProcessor;

    public IndexThread(String name, RestHighLevelClient client, BlockingQueue<Metric> metrics, String index, int batchSize, int flushInterval) {
        super(name);
        this.client = client;
        this.metrics = metrics;
        this.index = index;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;

        request = new MetricMultiGetRequest(index);

        bulkProcessor = BulkProcessor.builder(
                (request, bulkListener) ->
                        client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
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
                },
                "bulk-processor-name"
        )
                .setBulkActions(batchSize)
                .setFlushInterval(TimeValue.timeValueSeconds(flushInterval))
                .setConcurrentRequests(1)
                .build();
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                Metric metric = metrics.take();
                addToBatch(metric);
            } catch (Exception e) {
                logger.error("Encountered error in busy loop: ", e);
            }
        }

        if (request.size() > 0) {
            try {
                flush();
            } catch (Exception e) {
                logger.error("Encountered error in busy loop: ", e);
            }
        }
    }

    private void addToBatch(Metric metric) throws IOException {
        request.add(metric);

        if (request.size() >= batchSize || (lastFlushTimestamp < System.currentTimeMillis() / 1000L - flushInterval)) {
            flush();
            lastFlushTimestamp = System.currentTimeMillis() / 1000L;
        }
    }

    private void flush() throws IOException {
        MultiGetResponse multiGetResponse = client.mget(request, RequestOptions.DEFAULT);

        for (MultiGetItemResponse response : multiGetResponse.getResponses()) {
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
                        bulkProcessor.add(new IndexRequest(index).id(metric.getTenant() + "_" + sb.toString()).source(
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

        request = new MetricMultiGetRequest(index);
    }

    public void shutdown() {
        shutdown = true;
        this.interrupt();
    }

    private static class MetricMultiGetRequest extends MultiGetRequest {

        private final String index;
        final Map<String, Metric> metrics = new HashMap<>();


        public MetricMultiGetRequest(String index) {
            this.index = index;
        }

        public void add(Metric metric) {
            metrics.put(metric.getId(), metric);
            super.add(new MultiGetRequest.Item(index, metric.getId()).fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));
        }

        public int size() {
            return metrics.size();
        }
    }
}
