package reasoner;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import table.impl.ClassIndividuals;
import table.impl.IsSameAs;
import table.impl.PropIndividuals;

import java.util.HashMap;
import java.util.Optional;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

public abstract class ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerManager.class);

    public final Database connection;
    public final JavaSparkContext spark;
    public final JedisPool pool;

    public ReasonerManager(Database connection, SparkContext sc) {
        this.connection = connection;
        this.spark = JavaSparkContext.fromSparkContext(sc);
        this.pool = new JedisPool(new JedisPoolConfig());
    }

    public ReasonerManager(Database connection, SparkContext sc, JedisPool pool) {
        this.connection = connection;
        this.spark = JavaSparkContext.fromSparkContext(sc);
        this.pool = pool;
    }

    public ReasonerManager(Database connection, SparkConf conf) {
        this.connection = connection;
        this.spark = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        this.pool = new JedisPool(new JedisPoolConfig());
    }

    public ReasonerManager(Database connection, SparkConf conf, JedisPool pool) {
        this.connection = connection;
        this.spark = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        this.pool = pool;
    }

    public ReasonerManager(Database connection, JavaSparkContext sc) {
        this.connection = connection;
        this.spark = sc;
        this.pool = new JedisPool(new JedisPoolConfig());
    }

    public ReasonerManager(Database connection, JavaSparkContext sc, JedisPool pool) {
        this.connection = connection;
        this.spark = sc;
        this.pool = pool;
    }

    public Boolean insertToClassIndividuals(String cls, String individual, Jedis cache) {
        boolean referenceInserted = false;

        // Check if row exists
        String selectedExists = selectFrom(connection.getDatabaseName(), "classindividuals").all()
                .whereColumn("cls").isEqualTo(literal(cls))
                .whereColumn("individual").isEqualTo(literal(individual))
                .asCql();

        boolean rowExists = cache.exists(selectedExists);

        if (!rowExists) {
            // Get number of rows for this cls
            String occurrences = Optional.ofNullable(cache.get("ClassIndividuals_" + cls)).orElse("0");
            int num = Integer.parseInt(occurrences) + 1;

            // Insert to database
            HashMap<String, Object> assignments = new HashMap<>();
            assignments.put("cls", cls);
            assignments.put("num", num);
            assignments.put("individual", individual);

            SimpleStatement query = new ClassIndividuals(connection).statementInsert(assignments);
            connection.executeAsyncWithSession(query);

            // Cache query for future references
            cache.set(selectedExists, "1");
            cache.set("ClassIndividuals_" + cls, String.valueOf(num));

            referenceInserted = true;
        } else {
            LOGGER.debug("Row " + selectedExists + " already exists; skipping");
        }

        return referenceInserted;
    }

    public Boolean insertToPropIndividuals(String prop, String domain, String range, Jedis cache) {
        boolean referenceInserted = false;

        // Check if row exist
        String selectedExists = selectFrom(connection.getDatabaseName(), "propindividuals").all()
                .whereColumn("prop").isEqualTo(literal(prop))
                .whereColumn("domain").isEqualTo(literal(domain))
                .whereColumn("range").isEqualTo(literal(range))
                .asCql();

        boolean rowExists = cache.exists(selectedExists);

        if (!rowExists) {
            // Get number of rows for this individual
            String cachedNum = Optional.ofNullable(cache.get("PropIndividuals_" + prop)).orElse("0");
            int num = Integer.parseInt(cachedNum) + 1;

            // Insert to database
            HashMap<String, Object> assignments = new HashMap<>();
            assignments.put("prop", prop);
            assignments.put("num", num);
            assignments.put("domain", domain);
            assignments.put("range", range);

            SimpleStatement query = new PropIndividuals(connection).statementInsert(assignments);
            connection.executeAsyncWithSession(query);

            // Cache query for future references
            cache.set(selectedExists, "1");
            cache.set("PropIndividuals_" + prop, String.valueOf(num));

            referenceInserted = true;
        } else {
            LOGGER.debug("Row " + selectedExists + " already exists; skipping");
        }

        return referenceInserted;
    }

    public Boolean insertToSameAs(String a, String b, Jedis cache) {
        boolean referenceInserted = false;

        // Check if row exist
        String selectedExists = selectFrom(connection.getDatabaseName(), "issameas").all()
                .whereColumn("ind").isEqualTo(literal(a))
                .whereColumn("same").isEqualTo(literal(b))
                .asCql();

        boolean rowExists = cache.exists(selectedExists);

        if (!rowExists) {
            // Get number of rows for this individual
            String cachedNum = Optional.ofNullable(cache.get("IsSameAs_" + a)).orElse("0");
            int num = Integer.parseInt(cachedNum) + 1;

            // Insert to database
            HashMap<String, Object> assignments = new HashMap<>();
            assignments.put("ind", a);
            assignments.put("num", num);
            assignments.put("same", b);

            SimpleStatement query = new IsSameAs(connection).statementInsert(assignments);
            connection.executeAsyncWithSession(query);

            // Cache query for future references
            cache.set(selectedExists, "1");
            cache.set("IsSameAs_" + a, String.valueOf(num));

            referenceInserted = true;
        } else {
            LOGGER.debug("Row " + selectedExists + " already exists; skipping");
        }

        return referenceInserted;
    }

}
