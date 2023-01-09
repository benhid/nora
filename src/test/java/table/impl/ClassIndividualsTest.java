package table.impl;

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
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class ClassIndividualsTest {

    private ClassIndividuals classIndividuals;

    @Mock
    private Database mockedDatabase;

    @BeforeEach
    void setup() {
        Mockito.lenient().when(mockedDatabase.getDatabaseName()).thenReturn("test");
        classIndividuals = new ClassIndividuals(mockedDatabase);
    }

    @Test
    void shouldReturnDatabaseName() {
        assertEquals("test", classIndividuals.getDatabaseName());
    }

    @Test
    void shouldReturnStatementCreateTable() {
        CreateTableStart query = createTable(classIndividuals.getDatabaseName(), classIndividuals.getTableName()).ifNotExists();

        CreateTable finalQuery = query
                .withPartitionKey("cls", DataTypes.TEXT)
                .withClusteringColumn("num", DataTypes.INT)
                .withColumn("individual", DataTypes.TEXT);

        assertEquals(finalQuery.build(), classIndividuals.statementCreate());
    }

    @Test
    void shouldReturnEmptyStatementInsertTable() {
        InsertInto query = insertInto(classIndividuals.getDatabaseName(), classIndividuals.getTableName());
        RegularInsert regularInsertQuery = query
                .value("cls", bindMarker())
                .value("num", bindMarker())
                .value("individual", bindMarker());
        SimpleStatement expected = regularInsertQuery.build();

        assertEquals(expected.getNamedValues(), classIndividuals.statementInsert().getNamedValues());
    }

    @Test
    void shouldReturnStatementInsertTable() {
        HashMap<String, Object> assignments = new HashMap<>();
        assignments.put("cls", "cls_value");
        assignments.put("num", 1);
        assignments.put("individual", "individual_value");

        InsertInto query = insertInto(classIndividuals.getDatabaseName(), classIndividuals.getTableName());
        RegularInsert regularInsertQuery = query
                .value("cls", bindMarker())
                .value("num", bindMarker())
                .value("individual", bindMarker());
        SimpleStatement expected = regularInsertQuery.build(assignments);

        assertEquals(expected.getNamedValues(), classIndividuals.statementInsert(assignments).getNamedValues());
    }

    @Test
    void shouldReturnPrimaryKeyColumnNames() {
        Set<String> pkNames = new HashSet<>();
        pkNames.add("cls");
        pkNames.add("num");

        assertEquals(pkNames, classIndividuals.getPrimaryKeyColumnNames());
    }

    @Test
    void shouldReturnClusteringKeyColumnNames() {
        Set<String> clusteringNames = new HashSet<>();
        clusteringNames.add("num");

        assertEquals(clusteringNames, classIndividuals.getClusteringKeyColumnNames());
    }

    @Test
    void shouldReturnColumnsName() {
        Set<String> columnsName = new HashSet<>();
        columnsName.add("individual");

        assertEquals(columnsName, classIndividuals.getColumnsName());
    }

}