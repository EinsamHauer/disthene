package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrei Ivanov
 */
public class ESIndexStore implements IndexStore {
    private Logger logger = Logger.getLogger(ESIndexStore.class);

    private TransportClient client;
    private String index;
    private String type;
    private AtomicInteger storeCount = new AtomicInteger(0);
    private AtomicLong writeCount = new AtomicLong(0);
    private AtomicLong lastWriteCount = new AtomicLong(0);
    private BulkProcessor bulkProcessor;

    private BulkMetricProcessor processor;

    public ESIndexStore(DistheneConfiguration distheneConfiguration) throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", distheneConfiguration.getIndex().getName())
                .build();
        client = new TransportClient(settings);
        for (String node : distheneConfiguration.getIndex().getCluster()) {
            client.addTransportAddress(new InetSocketTransportAddress(node, distheneConfiguration.getIndex().getPort()));
        }

        index = distheneConfiguration.getIndex().getIndex();
        type = distheneConfiguration.getIndex().getType();

        bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                        writeCount.getAndAdd(response.getItems().length);
/*
                        int count = writeCount.addAndGet(response.getItems().length);

                        if (count % 1000 == 0) {
                            logger.info("Processed " + count + " metrics");
                        }
*/
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        logger.error(failure);
                    }
                })
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(4)
                .build();

        processor = new BulkMetricProcessor(client, index, type, 10000, TimeValue.timeValueSeconds(5));
    }

    @Override
    public void store(final Metric metric) {
        processor.add(metric);

//        client.prepareMultiGet();


//        bulkProcessor.add(new MetricSearchRequest(index, type, metric));
    }

    private class MetricSearchRequest extends GetRequest {
        private Metric metric;

        private MetricSearchRequest(String index, String type, Metric metric) {
            super(index, type, metric.getTenant() + "_" + metric.getPath());
            this.metric = metric;
        }

        private MetricSearchRequest(String index, String type, String id, Metric metric) {
            super(index, type, id);
            this.metric = metric;
        }

        public Metric getMetric() {
            return metric;
        }
    }

}


/*
        int count = storeCount.addAndGet(1);

        if (count % 1000 == 0) {
            logger.info("Stored " + count + " metrics");
        }
*/
/*        long count = writeCount.get();

        if (lastWriteCount.get() + 10000 < count) {
            lastWriteCount.getAndSet(count);
            logger.info("Written " + count + " paths");
        }

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
                e.printStackTrace();
            }
        }*/

