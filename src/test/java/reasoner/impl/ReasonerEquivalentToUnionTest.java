package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.ClassIndividuals;
import table.impl.IsEquivalentToUnion;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ReasonerEquivalentToUnionTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerEquivalentToUnionTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerEquivalentToUnionTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerEquivalentToUnion reasoner = new ReasonerEquivalentToUnion(connection, conf, mockedJedis);
        List<Tuple2<String, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(2, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>("#A", "#a")));
        assertTrue(inferences.contains(new Tuple2<>("#A", "#b")));
    }

    private void populate(Cassandra connection) {
        IsEquivalentToUnion isEquivalentToUnion = new IsEquivalentToUnion(connection);
        isEquivalentToUnion.initialize();

        SimpleStatement statement = isEquivalentToUnion.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#A");
            put("ind1", "#C");
            put("ind2", "#D");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        ClassIndividuals classIndividuals = new ClassIndividuals(connection);
        classIndividuals.initialize();

        statement = classIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#C");
            put("individual", "#a");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        statement = classIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#D");
            put("individual", "#b");
            put("num", 1);
        }});
        connection.getSession().execute(statement);
    }

}