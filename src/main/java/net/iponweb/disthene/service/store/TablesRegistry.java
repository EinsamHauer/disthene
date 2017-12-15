package net.iponweb.disthene.service.store;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import net.iponweb.disthene.config.StoreConfiguration;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Andrei Ivanov
 */
public class TablesRegistry {
    private Logger logger = Logger.getLogger(TablesRegistry.class);

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
            "  bloom_filter_fp_chance=%f AND\n" +
            "  caching='{\"keys\":\"ALL\"}' AND\n" +
            "  dclocal_read_repair_chance=%f AND\n" +
            "  gc_grace_seconds=%d AND\n" +
            "  read_repair_chance=%f AND\n" +
            "  memtable_flush_period_in_ms=%d AND\n" +
            "  compaction={'min_threshold': '%d', 'unchecked_tombstone_compaction': '%b', 'tombstone_threshold': '%f', 'class': '%s'} AND\n" +
            "  compression={'sstable_compression': '%s'};";

    private final Map<String, PreparedStatement> tables = new HashMap<>();
    private Session session;
    private StoreConfiguration storeConfiguration;
    private final PreparedStatement queryStatement;

    public TablesRegistry(Session session, StoreConfiguration storeConfiguration) {
        this.session = session;
        this.storeConfiguration = storeConfiguration;

        queryStatement = session.prepare(TABLE_QUERY);
    }

    public PreparedStatement getStatement(String tenant, int rollup) {
        logger.trace(String.format("Getting statement for %s, %d", tenant, rollup));

        String table = String.format(TABLE_TEMPLATE, tenant, rollup);
        if (tables.containsKey(table)) return tables.get(table);

        synchronized (this) {
            if (!tables.containsKey(table)) {
                // check if table exists
                logger.debug(String.format("Checking if table %s exists", table));

                if (!checkTable(table)) {
                    logger.debug(String.format("Table %s not found. Creating", table));

                    ResultSet resultSet = session.execute(getCreateTableQuery(table));
                    if (!resultSet.wasApplied()) {
                        throw new RuntimeException(String.format("Couldn't create table %s", table));
                    }
                    logger.debug(String.format("Created table %s. Preparing statement.", table));
                    tables.put(table, session.prepare(String.format(UPSERT_QUERY, storeConfiguration.getKeyspace(), table)));
                } else {
                    logger.debug(String.format("Found table %s. Preparing statement", table));
                    tables.put(table, session.prepare(String.format(UPSERT_QUERY, storeConfiguration.getKeyspace(), table)));
                }
            }
        }

        return tables.get(table);
    }

    private String getCreateTableQuery(String table) {
        return String.format(TABLE_CREATE,
                storeConfiguration.getKeyspace(),
                table,
                storeConfiguration.getFpChance(),
                storeConfiguration.getDclocalReadRepairChance(),
                storeConfiguration.getGcGraceSeconds(),
                storeConfiguration.getReadRepairChance(),
                storeConfiguration.getMemtableFlushPeriodInMs(),
                storeConfiguration.getCompactionMinThreshold(),
                storeConfiguration.isUncheckedTombstoneCompaction(),
                storeConfiguration.getTombstoneThreshold(),
                storeConfiguration.getCompactionClass(),
                storeConfiguration.getCompression()
                );
    }


    private boolean checkTable(String table) {
        ResultSet resultSet = session.execute(queryStatement.bind(storeConfiguration.getKeyspace(), table));
        return resultSet.one().getLong(0) > 0;
    }

}
