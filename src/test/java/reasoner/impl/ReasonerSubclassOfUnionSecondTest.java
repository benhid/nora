package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.ClassIndividuals;
import table.impl.IsComplementOf;
import table.impl.IsSubclassOfUnion;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ReasonerSubclassOfUnionSecondTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerSubclassOfUnionSecondTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerSubclassOfUnionSecondTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerSubclassOfUnionSecond reasoner = new ReasonerSubclassOfUnionSecond(connection, conf, mockedJedis);
        List<Tuple2<String, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(1, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>("#E", "#y")));
    }

    private void populate(Cassandra connection) {
        IsSubclassOfUnion isSubclassOfUnion = new IsSubclassOfUnion(connection);
        isSubclassOfUnion.initialize();

        SimpleStatement statement = isSubclassOfUnion.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#G");
            put("ind1", "#E");
            put("ind2", "#F");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        IsComplementOf isComplementOf = new IsComplementOf(connection);
        isComplementOf.initialize();

        statement = isComplementOf.statementInsert(new HashMap<String, Object>() {{
            put("key", "#J");
            put("complement", "#F");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        ClassIndividuals classIndividuals = new ClassIndividuals(connection);
        classIndividuals.initialize();

        statement = classIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#J");
            put("individual", "#y");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        statement = classIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#G");
            put("individual", "#y");
            put("num", 1);
        }});
        connection.getSession().execute(statement);
    }

}