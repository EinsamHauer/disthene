package net.iponweb.disthene.service.store;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import net.iponweb.disthene.config.StoreConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

/**
 * @author Andrei Ivanov
 */
public class TablesRegistry {
    private static final Logger logger = LogManager.getLogger(TablesRegistry.class);

    private static final String TABLE_QUERY = "SELECT COUNT(1) FROM SYSTEM_SCHEMA.TABLES WHERE KEYSPACE_NAME=? AND TABLE_NAME=?";
    private static final String UPSERT_QUERY = "UPDATE %s.%s USING TTL ? SET data = data + ? WHERE path = ? AND time = ?;";

    private final Map<String, PreparedStatement> tables = new HashMap<>();
    private final CqlSession session;
    private final StoreConfiguration storeConfiguration;
    private final PreparedStatement queryStatement;

    private final String tableTemplate;
    private final ConcurrentMap<String, String> tenants = new ConcurrentHashMap<>();
    private static final Pattern NORMALIZATION_PATTERN = Pattern.compile("[^0-9a-zA-Z_]");



    public TablesRegistry(CqlSession session, StoreConfiguration storeConfiguration) {
        this.session = session;
        this.storeConfiguration = storeConfiguration;
        this.tableTemplate = storeConfiguration.getTableTemplate();

        queryStatement = session.prepare(TABLE_QUERY);
    }

    public PreparedStatement getStatement(String tenant, int rollup) {
        logger.trace(String.format("Getting statement for %s, %d", tenant, rollup));

        String table = String.format(tableTemplate, getNormalizedTenant(tenant), rollup);
        if (tables.containsKey(table)) return tables.get(table);

        synchronized (this) {
            if (!tables.containsKey(table)) {
                // check if table exists
                logger.debug(String.format("Checking if table %s exists", table));

                if (!checkTable(table)) {
                    logger.debug(String.format("Table %s not found. Creating", table));

                    try {
                        ResultSet resultSet = session.execute(getCreateTableQuery(table));
                        if (!resultSet.wasApplied()) {
                            logger.error(String.format("Couldn't create table %s", table));
                        } else {
                            logger.debug(String.format("Created table %s. Preparing statement.", table));
                            tables.put(table, session.prepare(String.format(UPSERT_QUERY, storeConfiguration.getKeyspace(), table)));
                        }
                    } catch (Exception e) {
                        logger.error(String.format("Couldn't create table %s", table), e);
                    }
                } else {
                    logger.debug(String.format("Found table %s. Preparing statement", table));
                    tables.put(table, session.prepare(String.format(UPSERT_QUERY, storeConfiguration.getKeyspace(), table)));
                }
            }
        }

        return tables.get(table);
    }

    private String getCreateTableQuery(String table) {
        return String.format(storeConfiguration.getTableCreateTemplate(),
                storeConfiguration.getKeyspace(),
                table
                );
    }

    private boolean checkTable(String table) {
        ResultSet resultSet = session.execute(queryStatement.bind(storeConfiguration.getKeyspace(), table));
        return Objects.requireNonNull(resultSet.one()).getLong(0) > 0;
    }

    private String getNormalizedTenant(String tenant) {
        if (tenants.containsKey(tenant)) return tenants.get(tenant);

        String normalizedTenant = NORMALIZATION_PATTERN.matcher(tenant).replaceAll("_");
        tenants.putIfAbsent(tenant, normalizedTenant);
        return normalizedTenant;
    }
}
