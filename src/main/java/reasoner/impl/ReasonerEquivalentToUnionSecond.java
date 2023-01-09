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
import table.impl.IsEquivalentToUnion;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToUnionSecond extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToUnionSecond.class);

    public ReasonerEquivalentToUnionSecond(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToUnionSecond(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for EquivalentToUnionSecond");

        for (Tuple2<String, String> tuple : inferences) {
            String indIsEquivalentToUnion = tuple._1();
            String individualClassIndividuals = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(indIsEquivalentToUnion, individualClassIndividuals, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsEquivalentToUnion.Row> isEquivalentToUnionRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttounion", CassandraJavaUtil.mapRowTo(IsEquivalentToUnion.Row.class))
                .keyBy((Function<IsEquivalentToUnion.Row, String>) IsEquivalentToUnion.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isEquivalentToUnionRDD.foreach(data -> {
                LOGGER.debug("isequivalenttounion cls=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, IsComplementOf.Row> isComplementOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "iscomplementof", CassandraJavaUtil.mapRowTo(IsComplementOf.Row.class))
                .keyBy((Function<IsComplementOf.Row, String>) IsComplementOf.Row::getKey);

        if (LOGGER.isDebugEnabled())
            isComplementOfRDD.foreach(data -> {
                LOGGER.debug("iscomplementof key=" + data._1() + " row=" + data._2());
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

        JavaPairRDD<String, IsEquivalentToUnion.Row> firstJoinRDD = isEquivalentToUnionRDD
                .join(classIndividualsRDD)
                .mapToPair(Tuple2::_2)
                .mapToPair(Tuple2::swap);

        JavaPairRDD<String, IsComplementOf.Row> secondJoinRDD = isComplementOfRDD
                .join(classIndividualsRDD)
                .mapToPair(Tuple2::_2)
                .mapToPair(Tuple2::swap);

        JavaPairRDD<String, String> thirdJoinRDD = firstJoinRDD
                .join(secondJoinRDD)
                // Filters out rows when isEquivalentToUnionRow.(ind1|ind2) == isComplementOfRow.complement
                .filter(tuple -> {
                    IsEquivalentToUnion.Row isEquivalentToUnionRow = tuple._2()._1();
                    IsComplementOf.Row isComplementOfRow = tuple._2()._2();

                    boolean ind1EqualsComplement = isEquivalentToUnionRow.getInd1().equals(isComplementOfRow.getComplement());
                    boolean ind2EqualsComplement = isEquivalentToUnionRow.getInd2().equals(isComplementOfRow.getComplement());

                    return ind1EqualsComplement | ind2EqualsComplement;
                })
                // Returns pairs <ClassIndividuals.individual, IsEquivalentToUnion.(ind1|ind2)>
                .mapValues(tuple -> {
                    IsEquivalentToUnion.Row isEquivalentToUnionRow = tuple._1();
                    IsComplementOf.Row isComplementOfRow = tuple._2();

                    // We are interested in the complementary, thus if ind1 == complement => return ind2
                    if (isEquivalentToUnionRow.getInd1().equals(isComplementOfRow.getComplement())) {
                        return isEquivalentToUnionRow.getInd2();
                    } else {
                        return isEquivalentToUnionRow.getInd1();
                    }
                })
                .mapToPair(Tuple2::swap);

        return thirdJoinRDD.collect();
    }

}
