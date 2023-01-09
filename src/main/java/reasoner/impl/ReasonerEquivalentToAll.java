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
import table.impl.IsEquivalentToAll;
import table.impl.PropIndividuals;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToAll extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToAll.class);

    public ReasonerEquivalentToAll(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToAll(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for EquivalentToAll");

        for (Tuple2<String, String> tuple : inferences) {
            String rangeIsEquivalentToAll = tuple._1();
            String rangePropIndividuals = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(rangeIsEquivalentToAll, rangePropIndividuals, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsEquivalentToAll.Row> isEquivalentToAllRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttoall", CassandraJavaUtil.mapRowTo(IsEquivalentToAll.Row.class))
                .keyBy((Function<IsEquivalentToAll.Row, String>) IsEquivalentToAll.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isEquivalentToAllRDD.foreach(data -> {
                LOGGER.debug("isequivalenttoall cls=" + data._1() + " row=" + data._2());
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

        JavaPairRDD<String, Tuple3<IsEquivalentToAll.Row, String, String>> firstJoinRDD = isEquivalentToAllRDD
                .join(classIndividualsRDD)
                .mapToPair(tuple -> {
                    String cls = tuple._1();

                    Tuple2<IsEquivalentToAll.Row, String> secondOperand = tuple._2();
                    IsEquivalentToAll.Row isEquivalentToAllRow = secondOperand._1();
                    String individual = secondOperand._2();

                    return new Tuple2<>(isEquivalentToAllRow.getProp(), new Tuple3<>(isEquivalentToAllRow, individual, cls));
                });

        JavaPairRDD<String, String> secondJoinRDD = firstJoinRDD
                .join(propIndividualsRDD)
                .mapToPair(Tuple2::_2)
                // Filters out rows where individual == propIndividuals.Domain
                .filter(tuple -> {
                    String individual = tuple._1()._2();
                    PropIndividuals.Row propIndividualsRow = tuple._2();

                    return individual.equals(propIndividualsRow.getDomain());
                })
                // Transform to <range, range>
                .mapToPair(tuple -> {
                    IsEquivalentToAll.Row isEquivalentToAllRow = tuple._1()._1();
                    PropIndividuals.Row propIndividualsRow = tuple._2();

                    return new Tuple2<>(isEquivalentToAllRow.getRange(), propIndividualsRow.getRange());
                });

        return secondJoinRDD.collect();
    }

}
