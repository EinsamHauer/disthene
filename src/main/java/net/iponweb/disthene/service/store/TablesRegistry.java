package net.iponweb.disthene.service.store;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class TablesRegistry {
    private Logger logger = Logger.getLogger(CassandraService.class);

    private static final String TABLE_QUERY = "SELECT COUNT(1) FROM SYSTEM.SCHEMA_COLUMNFAMILIES WHERE KEYSPACE_NAME=? AND COLUMNFAMILY_NAME=?";
    private static final String TABLE_TEMPLATE = "%s_%d_metric";
    private static final String UPSERT_QUERY = "UPDATE %s.%s USING TTL ? SET data = data + ? WHERE path = ? AND time = ?;";

    // todo: configure table properties
    private static final String TABLE_CREATE = "CREATE TABLE IF NOT EXISTS %s.%s (\n" +
            "  path text,\n" +
            "  time bigint,\n" +
            "  data list<double>,\n" +
            "  PRIMARY KEY ((path), time)\n" +
            ") WITH\n" +
            "  bloom_filter_fp_chance=0.010000 AND\n" +
            "  caching='{\"keys\":\"ALL\"}' AND\n" +
            "  comment='' AND\n" +
            "  dclocal_read_repair_chance=0.100000 AND\n" +
            "  gc_grace_seconds=86400 AND\n" +
            "  read_repair_chance=0.100000 AND\n" +
            "  default_time_to_live=0 AND\n" +
            "  speculative_retry='NONE' AND\n" +
            "  memtable_flush_period_in_ms=60000 AND\n" +
            "  compaction={'min_threshold': '2', 'unchecked_tombstone_compaction': 'false', 'tombstone_threshold': '0.1', 'class': 'SizeTieredCompactionStrategy'} AND\n" +
            "  compression={'sstable_compression': 'LZ4Compressor'};";

    private final Map<String, PreparedStatement> tables = new HashMap<>();
    private Session session;
    private String keyspace;
    private final PreparedStatement queryStatement;

    public TablesRegistry(Session session, String keyspace) {
        this.session = session;
        this.keyspace = keyspace;

        queryStatement = session.prepare(TABLE_QUERY);
    }

    public PreparedStatement getStatement(String tenant, int rollup) {
        logger.trace(String.format("Getting statement for %s, %d", tenant, rollup));

        String table = String.format(TABLE_TEMPLATE, tenant, rollup);
        if (tables.containsKey(table)) return tables.get(table);

        createTable(table);


        return tables.get(table);
    }

    private synchronized void createTable(String table) {
        // double check
        if (tables.containsKey(table)) return;

        logger.info(String.format("Creating (if it's not there yet) table %s", table));

        try {
            ResultSet resultSet = session.execute(String.format(TABLE_CREATE, keyspace, table));
            if (!resultSet.wasApplied()) {
                throw new RuntimeException(String.format("Couldn't create table %s", table));
            }
            Thread.sleep(500);
            tables.put(table, session.prepare(String.format(UPSERT_QUERY, keyspace, table)));
        } catch (Exception e) {
            logger.error(String.format("Error creating table %s", table), e);
            ResultSet resultSet = session.execute(queryStatement.bind(keyspace, table));
            if (resultSet.one().getLong(0) <= 0) {
                throw new RuntimeException(String.format("The table should have been create but is not there: %s", table));
            }
        }
    }
}
