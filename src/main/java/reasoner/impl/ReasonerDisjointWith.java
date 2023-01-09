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
import table.impl.IsDisjointWith;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerDisjointWith extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerDisjointWith.class);

    public ReasonerDisjointWith(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerDisjointWith(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for DisjointWith");

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

        JavaPairRDD<String, IsDisjointWith.Row> isDisjointWithRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isdisjointwith", CassandraJavaUtil.mapRowTo(IsDisjointWith.Row.class))
                .keyBy((Function<IsDisjointWith.Row, String>) IsDisjointWith.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isDisjointWithRDD.foreach(data -> {
                LOGGER.debug("isdisjointwith cls=" + data._1() + " isdisjointwith=" + data._2());
            });

        JavaPairRDD<String, IsComplementOf.Row> isComplementOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "iscomplementof", CassandraJavaUtil.mapRowTo(IsComplementOf.Row.class))
                .keyBy((Function<IsComplementOf.Row, String>) IsComplementOf.Row::getKey);

        if (LOGGER.isDebugEnabled())
            isComplementOfRDD.foreach(data -> {
                LOGGER.debug("iscomplementof key=" + data._1() + " iscomplementof=" + data._2());
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

        JavaPairRDD<String, String> firstJoinRDD = isDisjointWithRDD
                .join(classIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<IsDisjointWith.Row, String> tuple = row._2();

                    IsDisjointWith.Row isDisjointWithRow = tuple._1();
                    String individual = tuple._2();

                    return new Tuple2<>(isDisjointWithRow.getInd1(), individual);
                });

        JavaPairRDD<String, String> secondJoinRDD = firstJoinRDD
                .join(isComplementOfRDD)
                .mapToPair(row -> {
                    Tuple2<String, IsComplementOf.Row> tuple = row._2();

                    String individual = tuple._1();
                    IsComplementOf.Row isComplementOfRow = tuple._2();

                    return new Tuple2<>(isComplementOfRow.getComplement(), individual);
                });

        return secondJoinRDD.collect();
    }

}
