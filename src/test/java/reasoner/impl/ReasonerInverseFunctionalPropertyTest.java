package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.IsInverseFunctionalProperty;
import table.impl.PropIndividuals;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class ReasonerInverseFunctionalPropertyTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerInverseFunctionalPropertyTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerInverseFunctionalPropertyTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerInverseFunctionalProperty reasoner = new ReasonerInverseFunctionalProperty(connection, conf, mockedJedis);
        List<Tuple2<String, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(2, inferences.size());

        assertEquals("#a1", inferences.get(0)._1);
        assertEquals("#a2", inferences.get(0)._2);

        assertEquals("#a2", inferences.get(1)._1);
        assertEquals("#a1", inferences.get(1)._2);
    }

    private void populate(Cassandra connection) {
        IsInverseFunctionalProperty isInverseFunctionalProperty = new IsInverseFunctionalProperty(connection);
        isInverseFunctionalProperty.initialize();

        SimpleStatement statement = isInverseFunctionalProperty.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
        }});
        connection.getSession().execute(statement);

        PropIndividuals propIndividuals = new PropIndividuals(connection);
        propIndividuals.initialize();

        statement = propIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
            put("domain", "#a1");
            put("range", "#b");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        statement = propIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
            put("domain", "#a2");
            put("range", "#b");
            put("num", 2);
        }});
        connection.getSession().execute(statement);
    }

}