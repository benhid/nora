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
import table.impl.IsSameAs;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerSameAsClassIndividual extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSameAsClassIndividual.class);

    public ReasonerSameAsClassIndividual(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSameAsClassIndividual(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SameAsClassIndividual");

        for (Tuple2<String, String> tuple : inferences) {
            String cls = tuple._1();
            String same = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(cls, same, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsSameAs.Row> isSameAsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issameas", CassandraJavaUtil.mapRowTo(IsSameAs.Row.class))
                .keyBy((Function<IsSameAs.Row, String>) IsSameAs.Row::getInd);

        if (LOGGER.isDebugEnabled())
            isSameAsRDD.foreach(data -> {
                LOGGER.debug("issameas ind=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, String> classIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getCls)
                .mapToPair(row -> {
                    String cls = row._1();
                    String individual = row._2().getIndividual();

                    return new Tuple2<>(individual, cls);
                });

        if (LOGGER.isDebugEnabled())
            classIndividualsRDD.foreach(data -> {
                LOGGER.debug("classindividuals individual=" + data._1() + " cls=" + data._2());
            });

        // Joins

        JavaPairRDD<String, String> firstJoinRDD = isSameAsRDD
                .join(classIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsSameAs.Row, String> secondOperand = tuple._2();
                    IsSameAs.Row isSameAs = secondOperand._1();
                    String cls = secondOperand._2();

                    return new Tuple2<>(cls, isSameAs.getSame());
                });

        return firstJoinRDD.collect();
    }

}
