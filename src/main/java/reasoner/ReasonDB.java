package reasoner;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import database.Configuration;
import database.DBInitException;
import database.Database;
import database.impl.Cassandra;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import reasoner.impl.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;

import java.util.List;
import java.util.Optional;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;


public class ReasonDB {

    private final static Logger LOGGER = Logger.getLogger(ReasonDB.class);

    private final JavaSparkContext spark;
    private final Database connection;
    private final JedisPool pool;

    public ReasonDB(Database connection, SparkConf conf, JedisPool pool) {
        this.connection = connection;
        this.spark = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        this.pool = pool;
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();

        Cassandra connection = new Cassandra(conf);

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        JedisPool pool = new JedisPool(jedisPoolConfig, conf.getProperty("redis_host"), Integer.parseInt(conf.getProperty("redis_port")), 10000);

        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkReasonerDB")
                //.setMaster("local[*]");
                .set("spark.driver.host", conf.getProperty("spark_host"))
                .set("spark.driver.maxResultSize", "0")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                // The connection pool closes the connection after 1 hour of inactivity by default.
                // We set it to 24 hours to avoid "CassandraConnector: Disconnected from Cassandra cluster" errors.
                .set("spark.cassandra.connection.keepAliveMS", "86400000");

        try {
            connection.connect();

            ReasonDB reasonDB = new ReasonDB(connection, sparkConf, pool);
            reasonDB.preloadCache();
            reasonDB.startLoop();
        } catch (DBInitException e) {
            LOGGER.error(e.getMessage());
        } finally {
            connection.disconnect();
        }
    }

    /**
     * Preloads cache with commonly queried tables. This step is not required.
     */
    public void preloadCache() {
        LOGGER.info("Populating cache");

        try (Jedis cache = pool.getResource()) {
            LOGGER.info("Flushing cache");
            cache.flushAll();
        }

        LOGGER.info("Pre-loading cache with ClassIndividuals...");

        Select query = selectFrom(connection.getDatabaseName(), "classindividuals").all();
        ResultSet rows = (ResultSet) connection.execute(query.build());
        int numberOfRows = 0;

        try (Jedis cache = pool.getResource()) {
            for (Row row : rows) {
                String columnClass = row.getString("cls");
                String columnIndividual = row.getString("individual");

                String count = Optional.ofNullable(cache.get("ClassIndividuals_" + columnClass)).orElse("0");
                int newCount = Integer.parseInt(count) + 1;

                String cachedQuery = selectFrom(connection.getDatabaseName(), "classindividuals").all()
                        .whereColumn("cls").isEqualTo(literal(columnClass))
                        .whereColumn("individual").isEqualTo(literal(columnIndividual))
                        .asCql();

                cache.set(cachedQuery, "1");
                cache.set("ClassIndividuals_" + columnClass, String.valueOf(newCount));

                numberOfRows++;
            }
        }

        LOGGER.info(String.format("Preloaded %s rows", numberOfRows));

        LOGGER.info("Pre-loading cache with PropIndividuals...");

        query = selectFrom(connection.getDatabaseName(), "propindividuals").all();
        rows = (ResultSet) connection.execute(query.build());
        numberOfRows = 0;

        try (Jedis cache = pool.getResource()) {
            for (Row row : rows) {
                String columnProp = row.getString("prop");
                String columnDomain = row.getString("domain");
                String columnRange = row.getString("range");

                String count = Optional.ofNullable(cache.get("PropIndividuals_" + columnProp)).orElse("0");
                int newCount = Integer.parseInt(count) + 1;

                String cachedQuery = selectFrom(connection.getDatabaseName(), "propindividuals").all()
                        .whereColumn("prop").isEqualTo(literal(columnProp))
                        .whereColumn("domain").isEqualTo(literal(columnDomain))
                        .whereColumn("range").isEqualTo(literal(columnRange))
                        .asCql();

                cache.set(cachedQuery, "1");
                cache.set("PropIndividuals_" + columnProp, String.valueOf(newCount));

                numberOfRows++;
            }
        }

        LOGGER.info(String.format("Preloaded %s rows", numberOfRows));

        LOGGER.info("Pre-loading cache with IsSameAs...");

        query = selectFrom(connection.getDatabaseName(), "issameas").all();
        rows = (ResultSet) connection.execute(query.build());
        numberOfRows = 0;

        try (Jedis cache = pool.getResource()) {
            for (Row row : rows) {
                String columnInd = row.getString("ind");
                String columnSame = row.getString("same");

                String count = Optional.ofNullable(cache.get("IsSameAs_" + columnInd)).orElse("0");
                int newCount = Integer.parseInt(count) + 1;

                String cachedQuery = selectFrom(connection.getDatabaseName(), "issameas").all()
                        .whereColumn("ind").isEqualTo(literal(columnInd))
                        .whereColumn("same").isEqualTo(literal(columnSame))
                        .asCql();

                cache.set(cachedQuery, "1");
                cache.set("IsSameAs_" + columnInd, String.valueOf(newCount));

                numberOfRows++;
            }
        }

        LOGGER.info(String.format("Preloaded %s rows", numberOfRows));
    }

    /**
     * Main loop. Reasoners are executed in a loop until no new instances are found.
     */
    public void startLoop() {
        ReasonerComplementOf complementOf = new ReasonerComplementOf(connection, spark, pool);
        ReasonerComplementOfSecond complementOfSecond = new ReasonerComplementOfSecond(connection, spark, pool);
        ReasonerDisjointWith disjointWith = new ReasonerDisjointWith(connection, spark, pool);
        ReasonerEquivalentToAll equivalentToAll = new ReasonerEquivalentToAll(connection, spark, pool);
        ReasonerEquivalentToClass equivalentToClass = new ReasonerEquivalentToClass(connection, spark, pool);
        ReasonerEquivalentToIntersection equivalentToIntersection = new ReasonerEquivalentToIntersection(connection, spark, pool);
        ReasonerEquivalentToIntersectionSecond equivalentToIntersectionSecond = new ReasonerEquivalentToIntersectionSecond(connection, spark, pool);
        ReasonerEquivalentToMaxCardinality equivalentToMaxCardinality = new ReasonerEquivalentToMaxCardinality(connection, spark, pool);
        ReasonerEquivalentToMinCardinality equivalentToMinCardinality = new ReasonerEquivalentToMinCardinality(connection, spark, pool);
        ReasonerEquivalentToSome equivalentToSome = new ReasonerEquivalentToSome(connection, spark, pool);
        ReasonerEquivalentToSomeSecond equivalentToSomeSecond = new ReasonerEquivalentToSomeSecond(connection, spark, pool);
        ReasonerEquivalentToUnion equivalentToUnion = new ReasonerEquivalentToUnion(connection, spark, pool);
        ReasonerEquivalentToUnionSecond equivalentToUnionSecond = new ReasonerEquivalentToUnionSecond(connection, spark, pool);
        ReasonerFunctionalProperty functionalProperty = new ReasonerFunctionalProperty(connection, spark, pool);
        ReasonerInverseFunctionalProperty inverseFunctionalProperty = new ReasonerInverseFunctionalProperty(connection, spark, pool);
        ReasonerObjectPropertyDomain opDomain = new ReasonerObjectPropertyDomain(connection, spark, pool);
        ReasonerObjectPropertyInverseOf opInverseOf = new ReasonerObjectPropertyInverseOf(connection, spark, pool);
        ReasonerObjectPropertyRange opRange = new ReasonerObjectPropertyRange(connection, spark, pool);
        ReasonerObjectPropertySubPropertyOf opSubPropertyOf = new ReasonerObjectPropertySubPropertyOf(connection, spark, pool);
        ReasonerSameAsClassIndividual sameAsClassIndividual = new ReasonerSameAsClassIndividual(connection, spark, pool);
        ReasonerSameAsPropIndividual sameAsPropIndividual = new ReasonerSameAsPropIndividual(connection, spark, pool);
        ReasonerSubclassOfAll subclassOfAll = new ReasonerSubclassOfAll(connection, spark, pool);
        ReasonerSubclassOfClass subclassOfClass = new ReasonerSubclassOfClass(connection, spark, pool);
        ReasonerSubclassOfIntersection subclassOfIntersection = new ReasonerSubclassOfIntersection(connection, spark, pool);
        ReasonerSubclassOfSomeSecond subclassOfSomeSecond = new ReasonerSubclassOfSomeSecond(connection, spark, pool);
        ReasonerSubclassOfUnionSecond subclassOfUnionSecond = new ReasonerSubclassOfUnionSecond(connection, spark, pool);

        LOGGER.info("Starting reasoning loop");

        boolean foundInferences = true;
        int stage = 1;

        while (foundInferences) {
            LOGGER.info("RUNNING STAGE #" + stage);

            Integer inferencesInserted;

            LOGGER.info("Starting complement family");

            List<Tuple2<String, String>> complementOfInferences = complementOf.inference();
            inferencesInserted = complementOf.resolve(complementOfInferences);
            complementOfInferences = null;

            List<Tuple2<String, String>> complementOfSecondInferences = complementOfSecond.inference();
            inferencesInserted += complementOfSecond.resolve(complementOfSecondInferences);
            complementOfSecondInferences = null;

            LOGGER.info("Starting disjoint with");

            List<Tuple2<String, String>> disjointWithInferences = disjointWith.inference();
            inferencesInserted += disjointWith.resolve(disjointWithInferences);
            disjointWithInferences = null;

            LOGGER.info("Starting object properties family");

            List<Tuple2<String, String>> opDomainInferences = opDomain.inference();
            inferencesInserted += opDomain.resolve(opDomainInferences);
            opDomainInferences = null;

            List<Tuple2<String, Tuple2<String, String>>> opInverseOfInferences = opInverseOf.inference();
            inferencesInserted += opInverseOf.resolve(opInverseOfInferences);
            opInverseOfInferences = null;

            List<Tuple2<String, String>> opRangeInferences = opRange.inference();
            inferencesInserted += opRange.resolve(opRangeInferences);
            opRangeInferences = null;

            List<Tuple2<String, Tuple2<String, String>>> opSubPropertyOfInferences = opSubPropertyOf.inference();
            inferencesInserted += opSubPropertyOf.resolve(opSubPropertyOfInferences);
            opSubPropertyOfInferences = null;

            LOGGER.info("Starting subclass family");

            List<Tuple2<String, String>> subclassOfClassInferences = subclassOfClass.inference();
            inferencesInserted += subclassOfClass.resolve(subclassOfClassInferences);
            subclassOfClassInferences = null;

            List<Tuple2<String, String>> subclassOfAllInferences = subclassOfAll.inference();
            inferencesInserted += subclassOfAll.resolve(subclassOfAllInferences);
            subclassOfAllInferences = null;

            List<Tuple2<Tuple2<String, String>, String>> subclassOfIntersectionInferences = subclassOfIntersection.inference();
            inferencesInserted += subclassOfIntersection.resolve(subclassOfIntersectionInferences);
            subclassOfIntersectionInferences = null;

            List<Tuple2<String, String>> subclassOfSomeSecondInferences = subclassOfSomeSecond.inference();
            inferencesInserted += subclassOfSomeSecond.resolve(subclassOfSomeSecondInferences);
            subclassOfSomeSecondInferences = null;

            List<Tuple2<String, String>> subclassOfUnionSecondInferences = subclassOfUnionSecond.inference();
            inferencesInserted += subclassOfUnionSecond.resolve(subclassOfUnionSecondInferences);
            subclassOfUnionSecondInferences = null;

            LOGGER.info("Starting equivalent family");

            List<Tuple2<String, String>> equivalentToAllInferences = equivalentToAll.inference();
            inferencesInserted += equivalentToAll.resolve(equivalentToAllInferences);
            equivalentToAllInferences = null;

            List<Tuple2<String, String>> equivalentToClassInferences = equivalentToClass.inference();
            inferencesInserted += equivalentToClass.resolve(equivalentToClassInferences);
            equivalentToClassInferences = null;

            List<Tuple2<Tuple2<String, String>, String>> equivalentToIntersectionInferences = equivalentToIntersection.inference();
            inferencesInserted += equivalentToIntersection.resolve(equivalentToIntersectionInferences);
            equivalentToIntersectionInferences = null;

            List<Tuple2<String, String>> equivalentToIntersectionSecondInferences = equivalentToIntersectionSecond.inference();
            inferencesInserted += equivalentToIntersectionSecond.resolve(equivalentToIntersectionSecondInferences);
            equivalentToIntersectionSecondInferences = null;

            List<Tuple2<String, String>> equivalentToSomeInferences = equivalentToSome.inference();
            inferencesInserted += equivalentToSome.resolve(equivalentToSomeInferences);
            equivalentToSomeInferences = null;

            List<Tuple2<String, String>> equivalentToSomeSecondInferences = equivalentToSomeSecond.inference();
            inferencesInserted += equivalentToSomeSecond.resolve(equivalentToSomeSecondInferences);
            equivalentToSomeSecondInferences = null;

            List<Tuple2<String, String>> equivalentToUnionInferences = equivalentToUnion.inference();
            inferencesInserted += equivalentToUnion.resolve(equivalentToUnionInferences);
            equivalentToUnionInferences = null;

            List<Tuple2<String, String>> equivalentToUnionSecondInferences = equivalentToUnionSecond.inference();
            inferencesInserted += equivalentToUnionSecond.resolve(equivalentToUnionSecondInferences);
            equivalentToUnionSecondInferences = null;

            // From here, all new inferences goes to IsSameAs

            List<Tuple2<String, String>> equivalentToMaxCardinalityInferences = equivalentToMaxCardinality.inference();
            inferencesInserted += equivalentToMaxCardinality.resolve(equivalentToMaxCardinalityInferences);
            equivalentToMaxCardinalityInferences = null;

            List<Tuple2<String, String>> equivalentToMinCardinalityInferences = equivalentToMinCardinality.inference();
            inferencesInserted += equivalentToMinCardinality.resolve(equivalentToMinCardinalityInferences);
            equivalentToMinCardinalityInferences = null;

            LOGGER.info("Starting functional properties");

            List<Tuple2<String, String>> functionalPropertyInferences = functionalProperty.inference();
            inferencesInserted += functionalProperty.resolve(functionalPropertyInferences);
            functionalPropertyInferences = null;

            List<Tuple2<String, String>> inverseFunctionalPropertyInferences = inverseFunctionalProperty.inference();
            inferencesInserted += inverseFunctionalProperty.resolve(inverseFunctionalPropertyInferences);
            inverseFunctionalPropertyInferences = null;

            LOGGER.info("Starting same as");

            List<Tuple2<String, String>> sameAsClassIndividualInferences = sameAsClassIndividual.inference();
            inferencesInserted += sameAsClassIndividual.resolve(sameAsClassIndividualInferences);
            sameAsClassIndividualInferences = null;

            List<Tuple2<String, Tuple2<String, String>>> sameAsPropIndividualInferences = sameAsPropIndividual.inference();
            inferencesInserted += sameAsPropIndividual.resolve(sameAsPropIndividualInferences);
            sameAsPropIndividualInferences = null;

            // TODO - http://www.java2s.com/example/java/big-data/transitive-closure-on-a-graph-via-apache-spark.html

            if (inferencesInserted == 0) {
                LOGGER.info("No new inferences yielded in stage " + stage);
                foundInferences = false;
            } else {
                LOGGER.info(inferencesInserted + " total inferences inserted in stage " + stage);
            }

            stage++;
        }
    }

}
