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
import table.impl.IsObjectPropertyRange;
import table.impl.PropIndividuals;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerObjectPropertyRange extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerObjectPropertyRange.class);

    public ReasonerObjectPropertyRange(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerObjectPropertyRange(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for ObjectPropertyRange");

        for (Tuple2<String, String> tuple : inferences) {
            String rangeRange = tuple._1();
            String rangeProperty = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(rangeRange, rangeProperty, cache))
                    inferencesInserted += 1;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables
        JavaPairRDD<String, IsObjectPropertyRange.Row> isObjectPropertyRangeRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isoprange", CassandraJavaUtil.mapRowTo(IsObjectPropertyRange.Row.class))
                .keyBy((Function<IsObjectPropertyRange.Row, String>) IsObjectPropertyRange.Row::getProperty);

        if (LOGGER.isDebugEnabled())
            isObjectPropertyRangeRDD.foreach(data -> {
                LOGGER.debug("isoprange property=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        if (LOGGER.isDebugEnabled())
            propIndividualsRDD.foreach(data -> {
                LOGGER.debug("propindividuals prop=" + data._1() + " row=" + data._2());
            });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = isObjectPropertyRangeRDD
                .join(propIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsObjectPropertyRange.Row, PropIndividuals.Row> secondOperand = tuple._2();

                    IsObjectPropertyRange.Row isObjectPropertyRangeRow = secondOperand._1();
                    PropIndividuals.Row propIndividualsRow = secondOperand._2();

                    return new Tuple2<>(isObjectPropertyRangeRow.getRange(), propIndividualsRow.getRange());
                });

        return firstJoinRDD.collect();
    }

}
