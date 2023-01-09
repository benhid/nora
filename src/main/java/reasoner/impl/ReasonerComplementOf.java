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
import table.impl.IsComplementOf;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerComplementOf extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerComplementOf.class);

    public ReasonerComplementOf(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerComplementOf(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for ComplementOf");

        for (Tuple2<String, String> tuple : inferences) {
            String complement = tuple._1();
            String individual = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(complement, individual, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsComplementOf.Row> isComplementOfByKeyRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "iscomplementof", CassandraJavaUtil.mapRowTo(IsComplementOf.Row.class))
                .keyBy((Function<IsComplementOf.Row, String>) IsComplementOf.Row::getKey);

        if (LOGGER.isDebugEnabled())
            isComplementOfByKeyRDD.foreach(data -> {
                LOGGER.debug("iscomplementof key=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, IsComplementOf.Row> isComplementOfByComplementRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "iscomplementof", CassandraJavaUtil.mapRowTo(IsComplementOf.Row.class))
                .keyBy((Function<IsComplementOf.Row, String>) IsComplementOf.Row::getComplement);

        if (LOGGER.isDebugEnabled())
            isComplementOfByComplementRDD.foreach(data -> {
                LOGGER.debug("iscomplementof complement=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, ClassIndividuals.Row> classIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getCls);

        if (LOGGER.isDebugEnabled())
            classIndividualsRDD.foreach(data -> {
                LOGGER.debug("classindividuals cls=" + data._1() + " row=" + data._2());
            });

        // Joins

        JavaPairRDD<String, IsComplementOf.Row> firstJoinRDD = isComplementOfByKeyRDD
                .join(isComplementOfByComplementRDD)
                .mapToPair(row -> {
                    Tuple2<IsComplementOf.Row, IsComplementOf.Row> tuple = row._2();

                    IsComplementOf.Row isComplementOfFirstRow = tuple._1();
                    IsComplementOf.Row isComplementOfSecondRow = tuple._2();

                    return new Tuple2<>(isComplementOfSecondRow.getKey(), isComplementOfFirstRow);
                });

        JavaPairRDD<String, String> secondJoinRDD = firstJoinRDD
                .join(classIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<IsComplementOf.Row, ClassIndividuals.Row> tuple = row._2();

                    IsComplementOf.Row isComplementOfRow = tuple._1();
                    ClassIndividuals.Row classIndividualsRow = tuple._2();

                    return new Tuple2<>(isComplementOfRow.getComplement(), classIndividualsRow.getIndividual());
                });

        return secondJoinRDD.collect();
    }

}
