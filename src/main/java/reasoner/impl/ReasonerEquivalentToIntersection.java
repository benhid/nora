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
import table.impl.IsEquivalentToIntersection;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToIntersection extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToIntersection.class);

    public ReasonerEquivalentToIntersection(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToIntersection(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<Tuple2<String, String>, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for EquivalentToIntersection");

        for (Tuple2<Tuple2<String, String>, String> tuple : inferences) {
            Tuple2<String, String> individuals = tuple._1();
            String individual = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(individuals._2(), individual, cache))
                    inferencesInserted++;
                if (insertToClassIndividuals(individuals._1(), individual, cache))
                    inferencesInserted++;
            }
        }

        // We are potentially inserting (totalNumberInferences * 2) new inferences
        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences * 2);

        return inferencesInserted;
    }

    public List<Tuple2<Tuple2<String, String>, String>> inference() {
        // Tables
        JavaPairRDD<String, IsEquivalentToIntersection.Row> isEquivalentToIntersectionRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttointersection", CassandraJavaUtil.mapRowTo(IsEquivalentToIntersection.Row.class))
                .keyBy((Function<IsEquivalentToIntersection.Row, String>) IsEquivalentToIntersection.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isEquivalentToIntersectionRDD.foreach(data -> {
                LOGGER.debug("isequivalenttointersection cls=" + data._1() + " row=" + data._2());
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
        JavaPairRDD<Tuple2<String, String>, String> firstJoinRDD = isEquivalentToIntersectionRDD
                .join(classIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<IsEquivalentToIntersection.Row, String> tuple = row._2();

                    IsEquivalentToIntersection.Row isEquivalentToIntersectionRow = tuple._1();
                    String individual = tuple._2();

                    String ind1 = isEquivalentToIntersectionRow.getInd1();
                    String ind2 = isEquivalentToIntersectionRow.getInd2();

                    Tuple2<String, String> intersection = new Tuple2<>(ind1, ind2);

                    return new Tuple2<>(intersection, individual);
                });

        return firstJoinRDD.collect();
    }

}
