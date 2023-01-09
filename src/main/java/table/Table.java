package table;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import database.Database;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class Table {

    protected String tableName;
    protected Database database;

    // Cluster opts
    protected Integer OntologyIndex;

    // Cassandra table definition
    protected HashMap<String, DataType> PartitionKeyColumns = new HashMap<>();
    protected HashMap<String, DataType> ClusteringKeyColumns = new HashMap<>();
    protected HashMap<String, DataType> Columns = new HashMap<>();

    public Table(String tableName, Database database) {
        this.tableName = tableName;
        this.database = database;
    }

    /**
     * Creates table and build prepared statements for frequently used queries.
     */
    public abstract void initialize();

    public abstract SimpleStatement statementIncrementalInsert(Map<String, Object> assignments);

    public abstract SimpleStatement statementInsert();

    public abstract SimpleStatement statementCreate();

    public Set<String> getColumnsName() {
        return Columns.keySet();
    }

    public HashMap<String, DataType> getColumns() {
        return Columns;
    }

    public Set<String> getPrimaryKeyColumnNames() {
        HashMap<String, DataType> merge = new HashMap<>();

        merge.putAll(PartitionKeyColumns);
        merge.putAll(ClusteringKeyColumns);

        return merge.keySet();
    }

    public HashMap<String, DataType> getPrimaryKeyColumns() {
        HashMap<String, DataType> merge = ClusteringKeyColumns;
        merge.putAll(PartitionKeyColumns);

        return merge;
    }

    public Set<String> getPartitionKeyColumnNames() {
        return PartitionKeyColumns.keySet();
    }

    public HashMap<String, DataType> getPartitionKeyColumns() {
        return PartitionKeyColumns;
    }

    public Set<String> getClusteringKeyColumnNames() {
        return ClusteringKeyColumns.keySet();
    }

    public HashMap<String, DataType> getClusteringKeyColumns() {
        return ClusteringKeyColumns;
    }

    public Set<String> getAllColumnsNames() {
        return getAllColumns().keySet();
    }

    public HashMap<String, DataType> getAllColumns() {
        HashMap<String, DataType> merge = PartitionKeyColumns;
        merge.putAll(ClusteringKeyColumns);
        merge.putAll(Columns);

        return merge;
    }

    public String getTableName() {
        return tableName;
    }

    public String getDatabaseName() {
        return database.getDatabaseName();
    }

}
