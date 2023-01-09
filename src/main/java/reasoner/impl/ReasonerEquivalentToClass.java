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
import table.impl.ClassIndividuals;
import table.impl.IsEquivalentToClass;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToClass extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToClass.class);

    public ReasonerEquivalentToClass(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToClass(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for EquivalentToClass");

        for (Tuple2<String, String> tuple : inferences) {
            String equiv = tuple._1();
            String individual = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(equiv, individual, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsEquivalentToClass.Row> isEquivalentToClassRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttoclass", CassandraJavaUtil.mapRowTo(IsEquivalentToClass.Row.class))
                .keyBy((Function<IsEquivalentToClass.Row, String>) IsEquivalentToClass.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isEquivalentToClassRDD.foreach(data -> {
                LOGGER.debug("isequivalenttoclass cls=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, String> classIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getCls)
                .mapToPair(row -> {
                    String cls = row._1();
                    String individual = row._2().getIndividual();

                    return new Tuple2<>(cls, individual);
                });

        if (LOGGER.isDebugEnabled())
            classIndividualsRDD.foreach(data -> {
                LOGGER.debug("classindividuals cls=" + data._1() + " individual=" + data._2());
            });

        // Joins

        JavaPairRDD<String, String> firstJoinRDD = isEquivalentToClassRDD
                .join(classIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsEquivalentToClass.Row, String> secondOperand = tuple._2();

                    IsEquivalentToClass.Row isEquivalentToClass = secondOperand._1();
                    String individual = secondOperand._2();

                    return new Tuple2<>(isEquivalentToClass.getEquiv(), individual);
                });

        return firstJoinRDD.collect();
    }

}
