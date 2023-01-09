package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.IsObjectPropertyInverseOf;
import table.impl.PropIndividuals;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ReasonerObjectPropertyInverseOfTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerObjectPropertyInverseOfTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerObjectPropertyInverseOfTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerObjectPropertyInverseOf reasoner = new ReasonerObjectPropertyInverseOf(connection, conf, mockedJedis);
        List<Tuple2<String, Tuple2<String, String>>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(1, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>("#Q", new Tuple2<>("#y", "#x"))));
    }

    private void populate(Cassandra connection) {
        IsObjectPropertyInverseOf isObjectPropertyInverseOf = new IsObjectPropertyInverseOf(connection);
        isObjectPropertyInverseOf.initialize();

        SimpleStatement statement = isObjectPropertyInverseOf.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
            put("inverse", "#Q");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        PropIndividuals propIndividuals = new PropIndividuals(connection);
        propIndividuals.initialize();

        statement = propIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("prop", "#P");
            put("domain", "#x");
            put("range", "#y");
            put("num", 1);
        }});
        connection.getSession().execute(statement);
    }

}