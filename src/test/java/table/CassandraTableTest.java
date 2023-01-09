package table;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import database.Database;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CassandraTableTest {

    private DummyTable mockedTable;

    @Mock
    private Database mockedDatabase;

    @BeforeEach
    void setup() {
        when(mockedDatabase.getDatabaseName()).thenReturn("test");
        mockedTable = new DummyTable("test", mockedDatabase);
    }

    @Test
    void shouldReturnDatabaseName() {
        assertEquals("test", mockedTable.getDatabaseName());
    }

    @Test
    void shouldReturnBuildStatementCreateTable() {
        CreateTableStart query = createTable(mockedTable.getDatabaseName(), mockedTable.getTableName()).ifNotExists();

        CreateTable createTableQuery = query
                .withPartitionKey("pk1", DataTypes.TEXT)
                .withClusteringColumn("ck1", DataTypes.TEXT)
                .withClusteringColumn("ck2", DataTypes.INT)
                .withColumn("c1", DataTypes.TEXT)
                .withColumn("c2", DataTypes.TEXT);
        SimpleStatement expected = createTableQuery.build();

        assertEquals(expected.getNamedValues(), mockedTable.statementCreate().getNamedValues());
    }

    @Test
    void shouldReturnStatementInsertWithValues() {
        HashMap<String, Object> assignments = new HashMap<>();
        assignments.put("pk1", "pk1_value");
        assignments.put("ck1", "ck1_value");

        InsertInto query = insertInto(mockedTable.getDatabaseName(), mockedTable.getTableName());
        RegularInsert regularInsertQuery = query
                .value("pk1", bindMarker())
                .value("ck1", bindMarker());
        SimpleStatement expected = regularInsertQuery.build(assignments);

        assertEquals(expected.getQuery(), "INSERT INTO test.dummy (pk1,ck1) VALUES (?,?)");
        assertEquals(expected.getNamedValues(), mockedTable.statementInsert(assignments).getNamedValues());
    }

    @Test
    void shouldReturnStatementWithIncrementedValues() {
        HashMap<String, Object> assignments = new HashMap<>();
        assignments.put("pk1", "pk1_value");
        assignments.put("ck1", "ck1_value");
        assignments.put("ck2", "ck2_value");

        SimpleStatement result = mockedTable.statementIncrementalInsert(assignments);

        assertEquals(result.getNamedValues().get(CqlIdentifier.fromCql("pk1")), "pk1_value");
        assertEquals(result.getNamedValues().get(CqlIdentifier.fromCql("ck1")), "ck1_value");
        assertEquals(result.getNamedValues().get(CqlIdentifier.fromCql("ck2")), "ck2_value");
        assertEquals(result.getNamedValues().get(CqlIdentifier.fromCql("num")), 1);

        SimpleStatement resultIncremented = mockedTable.statementIncrementalInsert(assignments);

        assertEquals(resultIncremented.getNamedValues().get(CqlIdentifier.fromCql("pk1")), "pk1_value");
        assertEquals(resultIncremented.getNamedValues().get(CqlIdentifier.fromCql("ck1")), "ck1_value");
        assertEquals(resultIncremented.getNamedValues().get(CqlIdentifier.fromCql("ck2")), "ck2_value");
        assertEquals(resultIncremented.getNamedValues().get(CqlIdentifier.fromCql("num")), 2);
    }

    private static class DummyTable extends CassandraTable {

        public DummyTable(String databaseName, Database database) {
            super("dummy", database);
            this.OntologyIndex = 0;

            this.PartitionKeyColumns.put("pk1", DataTypes.TEXT);
            this.ClusteringKeyColumns.put("ck1", DataTypes.TEXT);
            this.ClusteringKeyColumns.put("ck2", DataTypes.INT);

            this.Columns.put("c1", DataTypes.TEXT);
            this.Columns.put("c2", DataTypes.TEXT);
        }

    }

}