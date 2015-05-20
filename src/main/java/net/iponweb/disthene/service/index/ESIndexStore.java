package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;

/**
 * @author Andrei Ivanov
 */
public class ESIndexStore implements IndexStore {
    private Logger logger = Logger.getLogger(ESIndexStore.class);

    private TransportClient client;
    private XContentBuilder builder;
    private String index;
    private String type;

    public ESIndexStore(DistheneConfiguration distheneConfiguration) throws IOException {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", distheneConfiguration.getIndex().getName())
                .put("threadpool.search.type", "fixed").put("threadpool.search.size", 10)
                .put("threadpool.get.type", "fixed").put("threadpool.get.size", 10)
                .put("transport.connections_per_node.low", 0)
                .put("transport.connections_per_node.medium", 0)
                .put("transport.connections_per_node.high", 1)
//                .put("threadpool.generic.type", "fixed").put("threadpool.generic.size", 10)
                .build();
        client = new TransportClient(settings);
        for(String node : distheneConfiguration.getIndex().getCluster()) {
            client.addTransportAddress(new InetSocketTransportAddress(node, distheneConfiguration.getIndex().getPort()));
        }

        index = distheneConfiguration.getIndex().getIndex();
        type = distheneConfiguration.getIndex().getType();

        builder = XContentFactory.jsonBuilder();
    }

    @Override
    public void store(final Metric metric) throws IOException {
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
                    for(int i = 0; i < parts.length; i++) {
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
    }
}
