package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.config.DistheneConfiguration;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

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
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", distheneConfiguration.getIndex().getName()).build();
        client = new TransportClient(settings);
        for(String node : distheneConfiguration.getIndex().getCluster()) {
            client.addTransportAddress(new InetSocketTransportAddress(node, distheneConfiguration.getIndex().getPort()));
        }

        index = distheneConfiguration.getIndex().getIndex();
        type = distheneConfiguration.getIndex().getType();

        builder = XContentFactory.jsonBuilder();
    }

    @Override
    public void store(Metric metric) throws IOException {
        // Split paths
        String[] parts = metric.getPath().split("\\.");

        StringBuffer sb = new StringBuffer();
        for(int i = 0; i < parts.length; i++) {
            if (sb.toString().length() > 0) {
                sb.append(".");
            }
            sb.append(parts[i]);
/*
            logger.debug("Will store '" + sb.toString() + "' with tenant '" + metric.getTenant() + "' with depth " + (i + 1) + " and leaf set to " + (i == parts.length - 1));

            IndexResponse response = client.prepareIndex(index, type, metric.getTenant() + "_" + sb.toString())
                    .setSource(XContentFactory.jsonBuilder().startObject()
                                    .field("tenant", metric.getTenant())
                                    .field("path", sb.toString())
                                    .field("depth", (i + 1))
                                    .field("leaf", (i == parts.length - 1))
                                    .endObject()
                    )
                .execute()
                .actionGet();

            logger.debug(response);
*/
            client.prepareIndex(index, type, metric.getTenant() + "_" + sb.toString())
                    .setSource(XContentFactory.jsonBuilder().startObject()
                                    .field("tenant", metric.getTenant())
                                    .field("path", sb.toString())
                                    .field("depth", (i + 1))
                                    .field("leaf", (i == parts.length - 1))
                                    .endObject()
                    )
                    .execute();

        }

    }


}
