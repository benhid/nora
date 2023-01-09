package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.IsSameAs;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class ReasonerSameAsTransitiveClosureTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerSameAsClassIndividualTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerSameAsClassTransitiveClosureTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerSameAsTransitiveClosure reasoner = new ReasonerSameAsTransitiveClosure(connection, conf, mockedJedis);
        List<Tuple2<String, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions
        assertEquals(6, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>("#a", "#d")));
        assertTrue(inferences.contains(new Tuple2<>("#b", "#c")));
        assertTrue(inferences.contains(new Tuple2<>("#b", "#d")));
        assertTrue(inferences.contains(new Tuple2<>("#a", "#c")));
        assertTrue(inferences.contains(new Tuple2<>("#c", "#d")));
        assertTrue(inferences.contains(new Tuple2<>("#a", "#b")));
    }

    private void populate(Cassandra connection) {
        IsSameAs isSameAs = new IsSameAs(connection);
        isSameAs.initialize();

        SimpleStatement statement = isSameAs.statementInsert(new HashMap<String, Object>() {{
            put("ind", "#a");
            put("num", 1);
            put("same", "#b");
        }});
        connection.getSession().execute(statement);

        statement = isSameAs.statementInsert(new HashMap<String, Object>() {{
            put("ind", "#b");
            put("num", 1);
            put("same", "#c");
        }});
        connection.getSession().execute(statement);

        statement = isSameAs.statementInsert(new HashMap<String, Object>() {{
            put("ind", "#c");
            put("num", 1);
            put("same", "#d");
        }});
        connection.getSession().execute(statement);
    }

}