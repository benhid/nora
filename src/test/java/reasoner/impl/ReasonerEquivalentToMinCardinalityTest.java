package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.IsEquivalentToMinCardinality;
import table.impl.PropIndividuals;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ReasonerEquivalentToMinCardinalityTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerEquivalentToMinCardinalityTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerEquivalentToMinCardinalityTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerEquivalentToMinCardinality reasoner = new ReasonerEquivalentToMinCardinality(connection, conf, mockedJedis);
        List<Tuple2<String, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(1, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>("#A", "#a")));
    }

    private void populate(Cassandra connection) {
        IsEquivalentToMinCardinality isEquivalentToMinCardinality = new IsEquivalentToMinCardinality(connection);
        isEquivalentToMinCardinality.initialize();

        SimpleStatement statement = isEquivalentToMinCardinality.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#A");
            put("num", 1);
            put("prop", "#P");
            put("card", "1");
            put("clss", "http://www.w3.org/2002/07/owl#Thing");
        }});
        connection.getSession().execute(statement);

        PropIndividuals propIndividuals = new PropIndividuals(connection);
        propIndividuals.initialize();

        statement = propIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
            put("domain", "#a");
            put("range", "#b");
            put("num", 1);
        }});
        connection.getSession().execute(statement);
    }

}