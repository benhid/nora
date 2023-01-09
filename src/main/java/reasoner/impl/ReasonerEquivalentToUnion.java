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
import table.impl.IsEquivalentToUnion;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToUnion extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToUnion.class);

    public ReasonerEquivalentToUnion(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToUnion(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for EquivalentToUnion");

        for (Tuple2<String, String> tuple : inferences) {
            String cls = tuple._1();
            String individual = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(cls, individual, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsEquivalentToUnion.Row> isEquivalentToUnionByInd1RDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttounion", CassandraJavaUtil.mapRowTo(IsEquivalentToUnion.Row.class))
                .keyBy((Function<IsEquivalentToUnion.Row, String>) IsEquivalentToUnion.Row::getInd1);

        if (LOGGER.isDebugEnabled())
            isEquivalentToUnionByInd1RDD.foreach(data -> {
                LOGGER.debug("isequivalenttounion ind1=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, IsEquivalentToUnion.Row> isEquivalentToUnionByInd2RDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttounion", CassandraJavaUtil.mapRowTo(IsEquivalentToUnion.Row.class))
                .keyBy((Function<IsEquivalentToUnion.Row, String>) IsEquivalentToUnion.Row::getInd2);

        if (LOGGER.isDebugEnabled())
            isEquivalentToUnionByInd2RDD.foreach(data -> {
                LOGGER.debug("isequivalenttounion ind2=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, ClassIndividuals.Row> classIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getCls);

        if (LOGGER.isDebugEnabled())
            classIndividualsRDD.foreach(data -> {
                LOGGER.debug("classindividuals cls=" + data._1() + " row=" + data._2());
            });

        // Joins

        /*
        JavaPairRDD<String, String> thirdJoinRDD = firstJoinRDD
            .union(secondJoinRDD)
            .mapToPair(tuple -> {
                //String firstOperand = tuple._1();

                Tuple2<IsEquivalentToUnion.Row, ClassIndividuals.Row> secondOperand = tuple._2();
                IsEquivalentToUnion.Row isEquivalentToUnionRow = secondOperand._1();
                ClassIndividuals.Row classIndividualsRow = secondOperand._2();

                TraceableTuple2<String, String> tuple2 = new TraceableTuple2<>(isEquivalentToUnionRow.getCls(), classIndividualsRow.getIndividual());
                tuple2.appendTrace(String.format("Given %s and %s joined by IsEquivalentToUnion.ind=ClassIndividuals.cls", isEquivalentToUnionRow, classIndividualsRow));
                tuple2.appendTrace("then ClassIndividuals.ind is subclass of ClassIndividuals.ind");

                return tuple2;
            });

        // JUST FOR TESTING
        thirdJoinRDD.foreach(data -> {
            TraceableTuple2<String, String> traceableData = (TraceableTuple2<String, String>) data;
            LOGGER.info(traceableData.getTrace());
        }); */

        JavaPairRDD<String, Tuple2<IsEquivalentToUnion.Row, ClassIndividuals.Row>> firstJoinRDD = isEquivalentToUnionByInd1RDD
                .join(classIndividualsRDD);

        JavaPairRDD<String, Tuple2<IsEquivalentToUnion.Row, ClassIndividuals.Row>> secondJoinRDD = isEquivalentToUnionByInd2RDD
                .join(classIndividualsRDD);

        JavaPairRDD<String, String> thirdJoinRDD = firstJoinRDD
                .union(secondJoinRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsEquivalentToUnion.Row, ClassIndividuals.Row> secondOperand = tuple._2();
                    IsEquivalentToUnion.Row isEquivalentToUnionRow = secondOperand._1();
                    ClassIndividuals.Row classIndividualsRow = secondOperand._2();

                    return new Tuple2<>(isEquivalentToUnionRow.getCls(), classIndividualsRow.getIndividual());
                });

        return thirdJoinRDD.collect();
    }

}
