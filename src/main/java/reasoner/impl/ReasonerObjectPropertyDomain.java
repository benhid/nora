package reasoner.impl;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import database.Database;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import reasoner.ReasonerManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.Tuple2;
import table.impl.IsObjectPropertyDomain;
import table.impl.PropIndividuals;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerObjectPropertyDomain extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerObjectPropertyDomain.class);

    public ReasonerObjectPropertyDomain(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerObjectPropertyDomain(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for ObjectPropertyDomain");

        for (Tuple2<String, String> tuple : inferences) {
            String domainDomain = tuple._1();
            String domainProperty = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(domainDomain, domainProperty, cache))
                    inferencesInserted += 1;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables
        JavaPairRDD<String, IsObjectPropertyDomain.Row> isObjectPropertyDomainRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isopdomain", CassandraJavaUtil.mapRowTo(IsObjectPropertyDomain.Row.class))
                .keyBy((Function<IsObjectPropertyDomain.Row, String>) IsObjectPropertyDomain.Row::getProperty);

        if (LOGGER.isDebugEnabled())
            isObjectPropertyDomainRDD.foreach(data -> {
                LOGGER.debug("isopdomain property=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        if (LOGGER.isDebugEnabled())
            propIndividualsRDD.foreach(data -> {
                LOGGER.debug("propindividuals prop=" + data._1() + " row=" + data._2());
            });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = isObjectPropertyDomainRDD
                .join(propIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsObjectPropertyDomain.Row, PropIndividuals.Row> secondOperand = tuple._2();

                    IsObjectPropertyDomain.Row isObjectPropertyDomainRow = secondOperand._1();
                    PropIndividuals.Row propIndividualsRow = secondOperand._2();

                    return new Tuple2<>(isObjectPropertyDomainRow.getDomain(), propIndividualsRow.getDomain());
                });

        // todo Remove duplicates, if any

        return firstJoinRDD.collect();
    }
}
