package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.IsFunctionalProperty;
import table.impl.PropIndividuals;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ReasonerFunctionalPropertyTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerFunctionalPropertyTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerFunctionalPropertyTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerFunctionalProperty reasoner = new ReasonerFunctionalProperty(connection, conf, mockedJedis);
        List<Tuple2<String, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(2, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>("#b1", "#b2")));
        assertTrue(inferences.contains(new Tuple2<>("#b2", "#b1")));

        assertEquals("#b1", inferences.get(0)._1);
        assertEquals("#b2", inferences.get(0)._2);

        assertEquals("#b2", inferences.get(1)._1);
        assertEquals("#b1", inferences.get(1)._2);
    }

    private void populate(Cassandra connection) {
        IsFunctionalProperty isFunctionalProperty = new IsFunctionalProperty(connection);
        isFunctionalProperty.initialize();

        SimpleStatement statement = isFunctionalProperty.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
        }});
        connection.getSession().execute(statement);

        PropIndividuals propIndividuals = new PropIndividuals(connection);
        propIndividuals.initialize();

        statement = propIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
            put("domain", "#a");
            put("range", "#b1");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        statement = propIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
            put("domain", "#a");
            put("range", "#b2");
            put("num", 2);
        }});
        connection.getSession().execute(statement);

        statement = propIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
            put("domain", "#b");
            put("range", "#b3");
            put("num", 3);
        }});
        connection.getSession().execute(statement);
    }

}