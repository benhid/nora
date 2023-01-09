package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.ClassIndividuals;
import table.impl.IsSubclassOfIntersection;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ReasonerSubclassOfIntersectionTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerSubclassOfIntersectionTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerSubclassOfIntersectionTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerSubclassOfIntersection reasoner = new ReasonerSubclassOfIntersection(connection, conf, mockedJedis);
        List<Tuple2<Tuple2<String, String>, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(1, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>(new Tuple2<>("#B", "#C"), "#a")));
    }

    private void populate(Cassandra connection) {
        IsSubclassOfIntersection isSubclassOfIntersection = new IsSubclassOfIntersection(connection);
        isSubclassOfIntersection.initialize();

        SimpleStatement statement = isSubclassOfIntersection.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#A");
            put("ind1", "#B");
            put("ind2", "#C");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        ClassIndividuals classIndividuals = new ClassIndividuals(connection);
        classIndividuals.initialize();

        statement = classIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#A");
            put("individual", "#a");
            put("num", 1);
        }});
        connection.getSession().execute(statement);
    }

}