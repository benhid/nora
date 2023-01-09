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
import table.impl.IsSubclassOfClass;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerSubclassOfClass extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSubclassOfClass.class);

    public ReasonerSubclassOfClass(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSubclassOfClass(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SubclassOfClass");

        for (Tuple2<String, String> tuple : inferences) {
            String supclass = tuple._1();
            String individual = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(supclass, individual, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsSubclassOfClass.Row> isSubclassOfClassRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issubclassofclass", CassandraJavaUtil.mapRowTo(IsSubclassOfClass.Row.class))
                .keyBy((Function<IsSubclassOfClass.Row, String>) IsSubclassOfClass.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isSubclassOfClassRDD.foreach(data -> {
                LOGGER.debug("issubclassofclass cls=" + data._1() + " tab=" + data._2());
            });

        JavaPairRDD<String, ClassIndividuals.Row> classIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getCls);

        if (LOGGER.isDebugEnabled())
            classIndividualsRDD.foreach(data -> {
                LOGGER.debug("classindividuals cls=" + data._1() + " tab=" + data._2());
            });

        // Joins

        JavaPairRDD<String, String> firstJoinRDD = isSubclassOfClassRDD
                .join(classIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsSubclassOfClass.Row, ClassIndividuals.Row> secondOperand = tuple._2();

                    IsSubclassOfClass.Row isSubclassOfClassRow = secondOperand._1();
                    ClassIndividuals.Row classIndividualsRow = secondOperand._2();

                    return new Tuple2<>(isSubclassOfClassRow.getSupclass(), classIndividualsRow.getIndividual());
                });

        return firstJoinRDD.collect();
    }

}
