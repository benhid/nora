package reasoner.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.DBInitException;
import database.impl.Cassandra;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import reasoner.EmbeddedCassandra;
import scala.Tuple2;
import table.impl.ClassIndividuals;
import table.impl.IsComplementOf;
import table.impl.IsSubclassOfClass;

import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@ExtendWith(MockitoExtension.class)
public class ReasonerComplementOfSecondTest extends EmbeddedCassandra {

    @Test
    void shouldReturnInference() throws DBInitException {
        Cassandra connection = new Cassandra(host, port, username, password, "ReasonerComplementOfSecondTest");
        connection.connect();
        connection.createDatabaseIfNotExists();

        SparkConf conf = new SparkConf()
                .setAppName("ReasonerComplementOfSecondTest")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.allowMultipleContexts", "true");

        populate(connection);

        ReasonerComplementOfSecond reasoner = new ReasonerComplementOfSecond(connection, conf, mockedJedis);
        List<Tuple2<String, String>> inferences = reasoner.inference();

        connection.disconnect();

        // Assertions

        assertEquals(1, inferences.size());

        assertTrue(inferences.contains(new Tuple2<>("#I", "#a")));
    }

    private void populate(Cassandra connection) {
        ClassIndividuals classIndividuals = new ClassIndividuals(connection);
        classIndividuals.initialize();

        SimpleStatement statement = classIndividuals.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#J");
            put("individual", "#a");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        IsComplementOf isComplementOf = new IsComplementOf(connection);
        isComplementOf.initialize();

        statement = isComplementOf.statementInsert(new HashMap<String, Object>() {{
            put("complement", "#A");
            put("key", "#I");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        statement = isComplementOf.statementInsert(new HashMap<String, Object>() {{
            put("complement", "#B");
            put("key", "#J");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        IsSubclassOfClass isSubclassOfClass = new IsSubclassOfClass(connection);
        isSubclassOfClass.initialize();

        statement = isSubclassOfClass.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#A");
            put("supclass", "#B");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

        statement = isSubclassOfClass.statementInsert(new HashMap<String, Object>() {{
            put("cls", "#J");
            put("supclass", "#I");
            put("num", 1);
        }});
        connection.getSession().execute(statement);

    }

}