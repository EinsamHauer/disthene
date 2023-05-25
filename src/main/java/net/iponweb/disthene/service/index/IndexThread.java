package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import net.iponweb.disthene.bean.MetricPath;

import org.apache.http.cookie.SetCookie;
import org.apache.log4j.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.MgetRequest;
import org.opensearch.client.opensearch.core.MgetResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;
import org.opensearch.client.opensearch.core.mget.MultiGetOperation;
import org.opensearch.client.opensearch.core.mget.MultiGetResponseItem;
import org.opensearch.client.opensearch.core.mget.MultiGetResponseItemBuilders;
import org.opensearch.client.opensearch.core.mget.MultiGetError.Builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Andrei Ivanov
 */
public class IndexThread extends Thread {
    private Logger logger = Logger.getLogger(IndexThread.class);
    private OpenSearchClient client;
    private String index;
    private String type;
    private int batchSize;
    private int flushInterval;
    protected volatile boolean shutdown = false;
    protected Queue<Metric> metrics;
    Map<String, Metric> requests = new HashMap<>();
    private long lastFlushTimestamp = System.currentTimeMillis() / 1000L;
    private List mGetDocuments;

    public IndexThread(String name, OpenSearchClient client, Queue<Metric> metrics, String index, String type, int batchSize, int flushInterval) {
        super(name);
        this.client = client;
        this.metrics = metrics;
        this.index = index;
        this.type = type;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
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

        if (requests.size() > 0) {
            flush();
        }
    }

    private void addToBatch(Metric metric) {
        
        requests.put(this.hashPath(metric.getPath()), metric); ///add(metric);
        if (requests.size() >= batchSize || (lastFlushTimestamp < System.currentTimeMillis() / 1000L - flushInterval)) {
            flush();
            lastFlushTimestamp = System.currentTimeMillis() / 1000L;
        }
    }

    private void flush() {
        final Set<String> ids = requests.keySet();
        MgetRequest mgetreq = new MgetRequest.Builder()
                .index(index)
                .ids(new ArrayList<>(ids))
                .refresh(true)
                .build();

        List<MultiGetResponseItem> mGetDocuments = new ArrayList<MultiGetResponseItem>();
        try {
            MgetResponse mGetResponse = client.mget(mgetreq, Map.class);
            mGetDocuments = mGetResponse.docs();
        } catch (IOException e) {
            logger.error(e);
        }
        
        ArrayList<BulkOperation> ops = new ArrayList<>();
        for (MultiGetResponseItem m : mGetDocuments) {
            if(!m.result().found()) {
                final String id = m.result().id();
                final Metric metric = requests.get(id);
                final String[] parts = metric.getPath().split("\\.");
                final StringBuilder sb = new StringBuilder();
                for (int i = 0; i < parts.length; i++) {
                    if (sb.toString().length() > 0) {
                        sb.append(".");
                    }
                    sb.append(parts[i]);
                    String path = sb.toString();
                    final Map<String, Object> record = new TreeMap<>();
                    record.put("path", path);
                    record.put("depth", i + 1);
                    record.put("tenant", metric.getTenant());
                    record.put("leaf", (i == parts.length - 1));

                    ops.add(new BulkOperation.Builder().index(
                            IndexOperation.of(io -> io.index(index).id(id).document(record))
                    ).build());
                }
            }

        }
        BulkRequest.Builder bulkReq = new BulkRequest.Builder()
                .index(index)
                .operations(ops)
                .refresh(Refresh.WaitFor);
        try {
            BulkResponse bulkResponse = client.bulk(bulkReq.build());
            //for (BulkResponseItem bri : bulkResponse.items()) {
            //    logger.info(bri.status());
            //}
        } catch (IOException e) {
            logger.error(e);
        }
        requests = new HashMap<String, Metric>();
    }

    public void shutdown() {
        shutdown = true;
    }

    private String hashPath(String path) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(path.getBytes());
            BigInteger signum = new BigInteger(1, messageDigest);
            String hash = signum.toString(16);
            return hash;
        } catch (NoSuchAlgorithmException e) {
            logger.error("MD5: ", e);
            return "ERR";
        }
    }
}
