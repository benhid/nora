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
import table.impl.IsSubclassOfIntersection;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerSubclassOfIntersection extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSubclassOfIntersection.class);

    public ReasonerSubclassOfIntersection(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSubclassOfIntersection(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<Tuple2<String, String>, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SubclassOfIntersection");

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
        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<Tuple2<String, String>, String>> inference() {
        // Tables

        JavaPairRDD<String, IsSubclassOfIntersection.Row> isSubclassOfIntersectionRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issubclassofintersection", CassandraJavaUtil.mapRowTo(IsSubclassOfIntersection.Row.class))
                .keyBy((Function<IsSubclassOfIntersection.Row, String>) IsSubclassOfIntersection.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isSubclassOfIntersectionRDD.foreach(data -> {
                LOGGER.debug("issubclassofintersection cls=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, ClassIndividuals.Row> classIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getCls);

        if (LOGGER.isDebugEnabled())
            classIndividualsRDD.foreach(data -> {
                LOGGER.debug("classindividuals cls=" + data._1() + " row=" + data._2());
            });

        // Joins

        JavaPairRDD<Tuple2<String, String>, String> firstJoinRDD = isSubclassOfIntersectionRDD
                .join(classIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsSubclassOfIntersection.Row, ClassIndividuals.Row> secondOperand = tuple._2();

                    IsSubclassOfIntersection.Row isSubclassOfIntersectionRow = secondOperand._1();
                    ClassIndividuals.Row classIndividualsRow = secondOperand._2();

                    String ind1 = isSubclassOfIntersectionRow.getInd1();
                    String ind2 = isSubclassOfIntersectionRow.getInd2();

                    Tuple2<String, String> intersection = new Tuple2<>(ind1, ind2);

                    return new Tuple2<>(intersection, classIndividualsRow.getIndividual());
                });

        return firstJoinRDD.collect();
    }

}
