package net.iponweb.disthene.service.index;

import net.iponweb.disthene.bean.Metric;
import org.apache.log4j.Logger;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.opensearch.core.bulk.BulkOperation;
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem;
import org.opensearch.client.opensearch.core.bulk.IndexOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
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
    ArrayList<Metric> requests = new ArrayList<Metric>();
    private long lastFlushTimestamp = System.currentTimeMillis() / 1000L;

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
        requests.add(metric);
        if (requests.size() >= batchSize || (lastFlushTimestamp < System.currentTimeMillis() / 1000L - flushInterval)) {
            flush();
            lastFlushTimestamp = System.currentTimeMillis() / 1000L;
        }
    }

    private void flush() {
        ArrayList<BulkOperation> ops = new ArrayList<>();
        for (Metric m : requests) {
            final String[] parts = m.getPath().split("\\.");
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
                record.put("leaf", (i == parts.length - 1));

                ops.add(new BulkOperation.Builder().index(
                        IndexOperation.of(io -> io.index(index).id(hashPath(path)).document(record))
                ).build());
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
        requests = new ArrayList<Metric>();
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

    public void shutdown() {
        shutdown = true;
    }
}
