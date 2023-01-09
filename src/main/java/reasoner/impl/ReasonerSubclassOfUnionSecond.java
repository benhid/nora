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
import table.impl.IsSubclassOfUnion;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerSubclassOfUnionSecond extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSubclassOfUnionSecond.class);

    public ReasonerSubclassOfUnionSecond(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSubclassOfUnionSecond(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SubclassOfUnionSecond");

        for (Tuple2<String, String> tuple : inferences) {
            String indIsSubclassOfUnion = tuple._1();
            String individualClassIndividuals = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(indIsSubclassOfUnion, individualClassIndividuals, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsSubclassOfUnion.Row> isSubclassOfUnionRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issubclassofunion", CassandraJavaUtil.mapRowTo(IsSubclassOfUnion.Row.class))
                .keyBy((Function<IsSubclassOfUnion.Row, String>) IsSubclassOfUnion.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isSubclassOfUnionRDD.foreach(data -> {
                LOGGER.debug("issubclassofunion cls=" + data._1() + " row=" + data._2());
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

        JavaPairRDD<String, IsSubclassOfUnion.Row> a = isSubclassOfUnionRDD
                .join(classIndividualsRDD)
                .mapToPair(Tuple2::_2)
                .mapToPair(Tuple2::swap);

        JavaPairRDD<String, IsComplementOf.Row> b = isComplementOfRDD
                .join(classIndividualsRDD)
                .mapToPair(Tuple2::_2)
                .mapToPair(Tuple2::swap);

        JavaPairRDD<String, String> thirdJoinRDD = a
                .join(b)
                // Filters out rows when isSubclassOfUnionRow.(ind1|ind2) == isComplementOfRow.complement
                .filter(tuple -> {
                    IsSubclassOfUnion.Row isSubclassOfUnionRow = tuple._2()._1();
                    IsComplementOf.Row isComplementOfRow = tuple._2()._2();

                    boolean ind1EqualsComplement = isSubclassOfUnionRow.getInd1().equals(isComplementOfRow.getComplement());
                    boolean ind2EqualsComplement = isSubclassOfUnionRow.getInd2().equals(isComplementOfRow.getComplement());

                    return ind1EqualsComplement | ind2EqualsComplement;
                })
                // Returns pairs <ClassIndividuals.individual, IsSubclassOfUnion.(ind1|ind2)>
                .mapValues(tuple -> {
                    IsSubclassOfUnion.Row isSubclassOfUnionRow = tuple._1();
                    IsComplementOf.Row isComplementOfRow = tuple._2();

                    // We are interested in the complementary, thus if ind1 == complement => return ind2
                    if (isSubclassOfUnionRow.getInd1().equals(isComplementOfRow.getComplement())) {
                        return isSubclassOfUnionRow.getInd2();
                    } else {
                        return isSubclassOfUnionRow.getInd1();
                    }
                })
                .mapToPair(Tuple2::swap);

        return thirdJoinRDD.collect();
    }

}
