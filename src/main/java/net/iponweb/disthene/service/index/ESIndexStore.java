package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;

import javax.xml.ws.Response;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    private Executor executor = Executors.newFixedThreadPool(1000);
    private AtomicInteger storeCount = new AtomicInteger(0);
    private AtomicLong writeCount = new AtomicLong(0);
    private AtomicLong lastWriteCount = new AtomicLong(0);
    private BulkProcessor bulkProcessor;

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
    }

    @Override
    public void store(final Metric metric) throws IOException {
/*
        int count = storeCount.addAndGet(1);

        if (count % 1000 == 0) {
            logger.info("Stored " + count + " metrics");
        }
*/
        long count = writeCount.get();

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
        }

/*
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int count = storeCount.addAndGet(1);

                if (count % 1000 == 0) {
                    logger.info("Processed " + count + " metrics");
                }

                final String[] parts = metric.getPath().split("\\.");
                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                final StringBuilder sb = new StringBuilder();
                for (int i = 0; i < parts.length; i++) {
                    if (sb.toString().length() > 0) {
                        sb.append(".");
                    }
                    sb.append(parts[i]);

//            logger.debug("Storing to ES: " + sb.toString());
                    try {
                        bulkRequestBuilder.add(client.prepareUpdate(index, type, metric.getTenant() + "_" + sb.toString())
                                        .setUpsert(XContentFactory.jsonBuilder().startObject()
                                                .field("tenant", metric.getTenant())
                                                .field("path", sb.toString())
                                                .field("depth", (i + 1))
                                                .field("leaf", (i == parts.length - 1))
                                                .endObject())
                                        .setScript("", ScriptService.ScriptType.INLINE)
                        );
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                bulkRequestBuilder.execute().actionGet();
            }
        });
*/


//        executor.execute(new MetricStoreTask(metric));
/*
        client.prepareSearch("cyanite_paths")
                .setSize(1)
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("tenant", metric.getTenant()))
                        .must(QueryBuilders.termQuery("path", metric.getPath()))
                        .must(QueryBuilders.termQuery("leaf", true)))
                .execute().addListener(new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                logger.debug("ES returned " + searchResponse.getHits().totalHits() + " results");
                if (searchResponse.getHits().totalHits() == 0) {
                    // Split paths
                    final String[] parts = metric.getPath().split("\\.");

                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < parts.length; i++) {
                        if (sb.toString().length() > 0) {
                            sb.append(".");
                        }
                        sb.append(parts[i]);

                        logger.debug("Stroing to ES: " + sb.toString());

                        try {
                            client.prepareIndex(index, type, metric.getTenant() + "_" + sb.toString())
                                    .setSource(XContentFactory.jsonBuilder().startObject()
                                                    .field("tenant", metric.getTenant())
                                                    .field("path", sb.toString())
                                                    .field("depth", (i + 1))
                                                    .field("leaf", (i == parts.length - 1))
                                                    .endObject()
                                    )
                                    .execute();
                        } catch (Exception e) {
                            logger.error(e);
                        }
                    }
                }
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.error(throwable);
            }
        });
*/
    }

    private class MetricStoreTask implements Runnable {

        private Metric metric;

        public MetricStoreTask(Metric metric) {
            this.metric = metric;
        }

        @Override
        public void run() {
            int count = storeCount.addAndGet(1);


            if (count % 1000 == 0) {
                logger.debug("Processed " + count + " metrics");
            }

            try {
                SearchResponse response =
                        client.prepareSearch(index)
                                .setSize(1)
                                .setQuery(QueryBuilders.boolQuery()
                                        .must(QueryBuilders.termQuery("tenant", metric.getTenant()))
                                        .must(QueryBuilders.termQuery("path", metric.getPath()))
                                        .must(QueryBuilders.termQuery("leaf", true)))
                                .execute().actionGet();

                logger.debug("ES returned " + response.getHits().totalHits() + " results");
                if (response.getHits().totalHits() == 0) {
                    final String[] parts = metric.getPath().split("\\.");
                    BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                    final StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < parts.length; i++) {
                        if (sb.toString().length() > 0) {
                            sb.append(".");
                        }
                        sb.append(parts[i]);

                        logger.debug("Storing to ES: " + sb.toString());
/*
                        bulkRequestBuilder.add(client.prepareIndex(index, type, metric.getTenant() + "_" + sb.toString())
                                .setSource(XContentFactory.jsonBuilder().startObject()
                                                .field("tenant", metric.getTenant())
                                                .field("path", sb.toString())
                                                .field("depth", (i + 1))
                                                .field("leaf", (i == parts.length - 1))
                                                .endObject()
                                ));
*/

                        bulkRequestBuilder.add(client.prepareUpdate(index, type, metric.getTenant() + "_" + sb.toString())
                                .setUpsert(XContentFactory.jsonBuilder().startObject()
                                        .field("tenant", metric.getTenant())
                                        .field("path", sb.toString())
                                        .field("depth", (i + 1))
                                        .field("leaf", (i == parts.length - 1))
                                        .endObject())
                                .setScript("", ScriptService.ScriptType.INLINE)
                                );
/*
                        client.prepareIndex(index, type, metric.getTenant() + "_" + sb.toString())
                                .setSource(XContentFactory.jsonBuilder().startObject()
                                                .field("tenant", metric.getTenant())
                                                .field("path", sb.toString())
                                                .field("depth", (i + 1))
                                                .field("leaf", (i == parts.length - 1))
                                                .endObject()
                                )
                                .execute();
*/
                    }
                    BulkResponse response1 =  bulkRequestBuilder.execute().actionGet();
                    logger.debug(response1);
                }
            } catch (Exception e) {
                logger.error(e);
            }
        }
    }
}
