package database;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

public abstract class Database {

    protected final String host;
    protected final Integer port;
    protected final String username;
    protected final String password;
    protected final String databaseName;

    public Database(Configuration conf) {
        this.host = conf.getProperty("cassandra_host", "localhost");
        this.port = Integer.parseInt(conf.getProperty("cassandra_port", "9042"));
        this.username = conf.getProperty("cassandra_username", "cassandra");
        this.password = conf.getProperty("cassandra_password", "cassandra");
        this.databaseName = conf.getProperty("cassandra_database", "default");
    }

    public Database(String host, Integer port, String username, String password, String databaseName) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.databaseName = databaseName;
    }

    public abstract void connect() throws DBInitException;

    public abstract void disconnect();

    public abstract void createDatabaseIfNotExists();

    public abstract void dropDatabaseIfExists();

    public abstract Object execute(SimpleStatement statement);

    public abstract Object executeAsync(SimpleStatement statement);

    public abstract Object executeAsyncWithSession(SimpleStatement statement);

    public abstract Object getSession();

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabaseName() {
        return databaseName.toLowerCase();
    }

}
