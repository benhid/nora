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
import table.impl.IsSubclassOfClass;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerComplementOfSecond extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerComplementOfSecond.class);

    public ReasonerComplementOfSecond(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerComplementOfSecond(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for ComplementOfSecond");

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
        JavaPairRDD<String, IsSubclassOfClass.Row> isSubclassOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issubclassofclass", CassandraJavaUtil.mapRowTo(IsSubclassOfClass.Row.class))
                .keyBy((Function<IsSubclassOfClass.Row, String>) IsSubclassOfClass.Row::getCls);

        JavaPairRDD<String, IsComplementOf.Row> isComplementOfByComplementRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "iscomplementof", CassandraJavaUtil.mapRowTo(IsComplementOf.Row.class))
                .keyBy((Function<IsComplementOf.Row, String>) IsComplementOf.Row::getComplement);

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
        JavaPairRDD<String, IsComplementOf.Row> firstJoinRDD = isSubclassOfRDD
                .join(isComplementOfByComplementRDD)
                .mapToPair(row -> {
                    Tuple2<IsSubclassOfClass.Row, IsComplementOf.Row> tuple = row._2();

                    IsSubclassOfClass.Row isSubclassOfClassRow = tuple._1();
                    IsComplementOf.Row isComplementOfRow = tuple._2();

                    return new Tuple2<>(isSubclassOfClassRow.getSupclass(), isComplementOfRow);
                });

        JavaPairRDD<String, IsComplementOf.Row> secondJoinRDD = firstJoinRDD
                .join(isComplementOfByComplementRDD)
                .mapToPair(row -> {
                    Tuple2<IsComplementOf.Row, IsComplementOf.Row> tuple = row._2();

                    IsComplementOf.Row AisComplementOfRow = tuple._1();
                    IsComplementOf.Row BisComplementOfRow = tuple._2();

                    return new Tuple2<>(BisComplementOfRow.getKey(), AisComplementOfRow);
                });

        JavaPairRDD<String, String> thirdJoinRDD = secondJoinRDD
                .join(classIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<IsComplementOf.Row, String> tuple = row._2();

                    IsComplementOf.Row isComplementOfRow = tuple._1();
                    String individual = tuple._2();

                    return new Tuple2<>(isComplementOfRow.getKey(), individual);
                });

        return thirdJoinRDD.collect();
    }

}
