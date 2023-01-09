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
import scala.Tuple3;
import table.impl.ClassIndividuals;
import table.impl.IsSubclassOfAll;
import table.impl.PropIndividuals;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerSubclassOfAll extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSubclassOfAll.class);

    public ReasonerSubclassOfAll(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSubclassOfAll(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SubclassOfAll");

        for (Tuple2<String, String> tuple : inferences) {
            String rangeSubclassOfAll = tuple._1();
            String rangePropIndividuals = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(rangeSubclassOfAll, rangePropIndividuals, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsSubclassOfAll.Row> isSubclassOfAllRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issubclassofall", CassandraJavaUtil.mapRowTo(IsSubclassOfAll.Row.class))
                .keyBy((Function<IsSubclassOfAll.Row, String>) IsSubclassOfAll.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isSubclassOfAllRDD.foreach(data -> {
                LOGGER.debug("issubclassofall cls=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        if (LOGGER.isDebugEnabled())
            propIndividualsRDD.foreach(data -> {
                LOGGER.debug("propindividuals prop=" + data._1() + " row=" + data._2());
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

        JavaPairRDD<String, Tuple3<IsSubclassOfAll.Row, String, String>> firstJoinRDD = isSubclassOfAllRDD
                .join(classIndividualsRDD)
                .mapToPair(tuple -> {
                    String cls = tuple._1();

                    Tuple2<IsSubclassOfAll.Row, String> secondOperand = tuple._2();
                    String individual = secondOperand._2();
                    IsSubclassOfAll.Row isSubclassOfAllRow = secondOperand._1();

                    return new Tuple2<>(isSubclassOfAllRow.getProp(), new Tuple3<>(isSubclassOfAllRow, individual, cls));
                });

        JavaPairRDD<String, String> secondJoinRDD = firstJoinRDD
                .join(propIndividualsRDD)
                .mapToPair(Tuple2::_2)
                // Filters out rows where individual == domain
                .filter(tuple -> {
                    Tuple3<IsSubclassOfAll.Row, String, String> firstOperand = tuple._1();
                    String individual = firstOperand._2();

                    PropIndividuals.Row propIndividualsRow = tuple._2();

                    return individual.equals(propIndividualsRow.getDomain());
                })
                // Transform to <range, range>
                .mapToPair(tuple -> {
                    Tuple3<IsSubclassOfAll.Row, String, String> firstOperand = tuple._1();
                    IsSubclassOfAll.Row isSubclassOfAllRow = firstOperand._1();

                    PropIndividuals.Row propIndividualsRow = tuple._2();

                    return new Tuple2<>(isSubclassOfAllRow.getRange(), propIndividualsRow.getRange());
                });

        return secondJoinRDD.collect();
    }

}
