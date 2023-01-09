package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.ClassIndividuals;
import table.impl.IsEquivalentToIntersection;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ReasonerEquivalentToIntersectionSecondTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerEquivalentToIntersectionSecondTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerEquivalentToIntersectionSecondTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerEquivalentToIntersectionSecond reasoner = new ReasonerEquivalentToIntersectionSecond(connection, conf, mockedJedis);
        List<Tuple2<String, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(1, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>("#C", "#a")));
    }

    private void populate(Cassandra connection) {
        ClassIndividuals classIndividuals = new ClassIndividuals(connection);
        classIndividuals.initialize();

        SimpleStatement statement = classIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#A");
            put("individual", "#a");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        statement = classIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#B");
            put("individual", "#a");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        IsEquivalentToIntersection isEquivalentToIntersection = new IsEquivalentToIntersection(connection);
        isEquivalentToIntersection.initialize();

        statement = isEquivalentToIntersection.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#C");
            put("ind1", "#A");
            put("ind2", "#B");
            put("num", 1);
        }});
        connection.getSession().execute(statement);
    }

}