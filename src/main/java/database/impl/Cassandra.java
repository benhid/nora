package database.impl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import database.Configuration;
import database.DBInitException;
import database.Database;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Semaphore;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createKeyspace;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.dropKeyspace;

public class Cassandra extends Database {

    private final static Logger LOGGER = Logger.getLogger(Cassandra.class);
    private final Semaphore semaphore = new Semaphore(300);

    // Cache for bound statements.
    private final Map<String, PreparedStatement> cache;

    // Connection details.
    private CqlSession session;

    public Cassandra(Configuration conf) {
        super(conf);
        this.cache = new HashMap<>();
    }

    public Cassandra(String host, Integer port, String username, String password, String databaseName) {
        super(host, port, username, password, databaseName);
        this.cache = new HashMap<>();
    }

    @Override
    public void connect() throws DBInitException {
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, getUsername())
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, getPassword())
                .withString(DefaultDriverOption.REQUEST_TIMEOUT, "60s")
                .build();

        LOGGER.info(String.format("Connecting to Cassandra at %s:%s", getHost(), getPort()));

        try {
            session = CqlSession.builder()
                    .withConfigLoader(loader)
                    .withLocalDatacenter("datacenter1")
                    .addContactPoint(new InetSocketAddress(getHost(), getPort()))
                    .build();
            LOGGER.info("Connection established to database");
        } catch (Exception e) {
            throw new DBInitException(e.getMessage() + " " + e.getCause());
        }
    }

    @Override
    public void disconnect() {
        if (session != null && !session.isClosed())
            session.close();
    }

    /**
     * Deletes keyspace if exists previously.
     */
    @Override
    public void dropDatabaseIfExists() {
        execute(dropKeyspace(getDatabaseName()).ifExists().build());
        LOGGER.info(String.format("Deleted keyspace %s", getDatabaseName()));
    }

    /**
     * Creates the keyspace if not exists.
     */
    @Override
    public void createDatabaseIfNotExists() {
        execute(createKeyspace(getDatabaseName()).ifNotExists().withSimpleStrategy(1).build());
        LOGGER.info(String.format("Set working keyspace %s", getDatabaseName()));
    }

    @Override
    public ResultSet execute(final SimpleStatement statement) {
        LOGGER.debug(String.format("%s %s", statement.getQuery(), statement.getNamedValues()));
        return session.execute(statement);
    }

    /**
     * Executes a CQL query implementing semaphore strategy to throttle async writes
     * for congestion control.
     * See also <a href=
     * "https://stackoverflow.com/questions/30509095/cassandra-is-there-a-way-to-limit-number-of-async-queries">this</a>
     * and <a href=
     * "https://community.datastax.com/questions/4480/error-during-select-aggregated-query-with-group-by.html">this</a>.
     *
     * @param statement the CQL query to execute (that can be any
     *                  {@code Statement}).
     * @return a {@code CompletionStage}.
     */
    @Override
    public CompletionStage<AsyncResultSet> executeAsync(final SimpleStatement statement) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.debug(String.format("%s %s", statement.getQuery(), statement.getNamedValues()));
        return session.executeAsync(statement).whenComplete(
                (resultSet, error) -> {
                    if (error != null)
                        error.printStackTrace();
                    semaphore.release();
                });
    }

    /**
     * Executes a bound CQL query implementing semaphore strategy to throttle async
     * writes for congestion control.
     * See also <a href=
     * "https://stackoverflow.com/questions/30509095/cassandra-is-there-a-way-to-limit-number-of-async-queries">this</a>
     * and <a href=
     * "https://community.datastax.com/questions/4480/error-during-select-aggregated-query-with-group-by.html">this</a>.
     *
     * @param statement the CQL query to execute (that can be any
     *                  {@code Statement}).
     * @return a {@code CompletionStage}.
     */
    @Override
    public CompletionStage<AsyncResultSet> executeAsyncWithSession(final SimpleStatement statement) {
        LOGGER.debug(String.format("%s %s", statement.getQuery(), statement.getNamedValues()));
        return executeAsyncWithSession(statement.getQuery(), statement.getNamedValues());
    }

    private CompletionStage<AsyncResultSet> executeAsyncWithSession(final String cql,
                                                                    final Map<CqlIdentifier, Object> values) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        BoundStatement bs = getBoundStatement(cql, values);
        return session.executeAsync(bs).whenComplete(
                (resultSet, error) -> {
                    if (error != null)
                        error.printStackTrace();
                    semaphore.release();
                });
    }

    /**
     * Gets bound statement from cache based on CQL query.
     *
     * @param cql    the CQL query
     * @param values the values
     * @return a {@code BoundStatement}
     */
    private BoundStatement getBoundStatement(final String cql, Map<CqlIdentifier, Object> values) {
        PreparedStatement ps = cache.get(cql);
        // No statement cached, create one and cache it now.
        if (ps == null) {
            ps = session.prepare(cql);
            PreparedStatement old = cache.putIfAbsent(cql, ps);
            if (old != null)
                ps = old;
        }
        // Bind named values to bound statement
        BoundStatement bs = ps.bind();
        for (Map.Entry<CqlIdentifier, Object> entry : values.entrySet()) {
            CqlIdentifier identifier = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof String)
                bs = bs.setString(identifier, (String) value);
            else if (value instanceof Integer)
                bs = bs.setInt(identifier, (Integer) value);
            else if (value instanceof Double)
                bs = bs.setDouble(identifier, (Double) value);
            else if (value instanceof Boolean)
                bs = bs.setBoolean(identifier, (Boolean) value);
            else
                LOGGER.error(String.format("Named value %s can not be binded to bound statement", identifier));
        }
        return bs;
    }

    @Override
    public CqlSession getSession() {
        return session;
    }

}
