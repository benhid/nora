package table;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import database.Database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;

public abstract class CassandraTable extends Table {

    /**
     * Apache Cassandra does not implements auto-incremental columns.
     */
    protected final HashMap<List<Object>, Integer> cache;

    public CassandraTable(String tableName, Database database) {
        super(tableName, database);
        cache = new HashMap<>();
    }

    /**
     * Creates table and build prepared statements for frequently used queries.
     */
    public void initialize() {
        database.execute(statementCreate());
    }

    public SimpleStatement statementIncrementalInsert(Map<String, Object> assignments) {
        // Gets assignments' primary keySet by taking all keys from `assignments` which are PKs of this table
        Map<String, Object> pk = new HashMap<>(assignments);
        pk.keySet().retainAll(getPrimaryKeyColumnNames());

        // Builds new bound insert statement
        RegularInsert statement = statementInsertWithKeyspace(database.getDatabaseName());

        // Cache primary keySet and increments instance counter
        cache.put(new ArrayList<>(pk.values()), cache.getOrDefault(new ArrayList<>(pk.values()), 0) + 1);

        Map<String, Object> namedValues = new HashMap<>(assignments);
        namedValues.put("num", cache.get(new ArrayList<>(pk.values())));

        return statement.build(namedValues);
    }

    public SimpleStatement statementInsert(Map<String, Object> assignments) {
        return statementInsertWithKeyspace(database.getDatabaseName()).build(assignments);
    }

    public SimpleStatement statementInsert() {
        return statementInsertWithKeyspace(database.getDatabaseName()).build();
    }

    private RegularInsert statementInsertWithKeyspace(String keyspace) {
        InsertInto query = insertInto(keyspace, getTableName());

        for (String columnName : getPrimaryKeyColumnNames())
            query = (InsertInto) query.value(columnName, bindMarker());

        RegularInsert regularInsert = (RegularInsert) query;

        for (String columnName : getColumnsName())
            regularInsert = regularInsert.value(columnName, bindMarker());

        //String template = regularInsert.asCql();

        return regularInsert;
    }

    public SimpleStatement statementCreate() {
        return statementCreateWithKeyspace(getDatabaseName()).build();
    }

    private CreateTable statementCreateWithKeyspace(String keyspace) {
        CreateTableStart query = createTable(keyspace, getTableName()).ifNotExists();

        // Adds a partition key column definition
        for (Map.Entry<String, DataType> column : getPartitionKeyColumns().entrySet())
            query = (CreateTableStart) query.withPartitionKey(column.getKey(), column.getValue());

        CreateTable finalQuery = (CreateTable) query;

        for (Map.Entry<String, DataType> column : getClusteringKeyColumns().entrySet())
            finalQuery = finalQuery.withClusteringColumn(column.getKey(), column.getValue());

        // Adds a static column definition in the statement
        for (Map.Entry<String, DataType> column : getColumns().entrySet())
            finalQuery = finalQuery.withColumn(column.getKey(), column.getValue());

        return finalQuery;
    }

}