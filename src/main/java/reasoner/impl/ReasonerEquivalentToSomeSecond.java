package reasoner.impl;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import database.Database;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import reasoner.ReasonerManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import scala.Tuple2;
import table.impl.ClassIndividuals;
import table.impl.IsEquivalentToSome;
import table.impl.IsFunctionalProperty;
import table.impl.PropIndividuals;

import java.util.List;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToSomeSecond extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToSomeSecond.class);

    public ReasonerEquivalentToSomeSecond(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToSomeSecond(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for EquivalentToSome");

        for (Tuple2<String, String> tuple : inferences) {
            String rangeIsEquivalentToSome = tuple._1();
            String rangePropIndividuals = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(rangeIsEquivalentToSome, rangePropIndividuals, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsEquivalentToSome.Row> isEquivalentToSomeRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttosome", CassandraJavaUtil.mapRowTo(IsEquivalentToSome.Row.class))
                .keyBy((Function<IsEquivalentToSome.Row, String>) IsEquivalentToSome.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isEquivalentToSomeRDD.foreach(data -> {
                LOGGER.debug("isequivalenttosome cls=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        if (LOGGER.isDebugEnabled())
            propIndividualsRDD.foreach(data -> {
                LOGGER.debug("propindividuals prop=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, IsFunctionalProperty.Row> isFunctionalPropertyRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isfunctionalproperty", CassandraJavaUtil.mapRowTo(IsFunctionalProperty.Row.class))
                .keyBy((Function<IsFunctionalProperty.Row, String>) IsFunctionalProperty.Row::getProp);

        if (LOGGER.isDebugEnabled())
            isFunctionalPropertyRDD.foreach(data -> {
                LOGGER.debug("isfunctionalproperty prop=" + data._1() + " row=" + data._2());
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

        JavaPairRDD<String, Tuple2<IsEquivalentToSome.Row, String>> firstJoinRDD = isEquivalentToSomeRDD
                .join(classIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsEquivalentToSome.Row, String> secondOperand = tuple._2();
                    IsEquivalentToSome.Row isEquivalentToSomeRow = secondOperand._1();
                    String individual = secondOperand._2();

                    return new Tuple2<>(isEquivalentToSomeRow.getProp(), new Tuple2<>(isEquivalentToSomeRow, individual));
                });

        // This case is somewhat similar to the case of `ReasonerSubclassOfSomeSecond`.
        // We will apply the same optimizations:

        List<String> keys = firstJoinRDD.keys().distinct().collect();

        JavaPairRDD<String, PropIndividuals.Row> reducedRDD = propIndividualsRDD
                .filter(tuple -> keys.contains(tuple._1()));

        Broadcast<Map<String, Tuple2<IsEquivalentToSome.Row, String>>> fastLookup = this.spark.broadcast(firstJoinRDD.collectAsMap());

        JavaPairRDD<String, Tuple2<IsEquivalentToSome.Row, PropIndividuals.Row>> secondJoinRDD = reducedRDD
                // Map-side join
                .mapToPair(tuple -> {
                    String prop = tuple._1();
                    PropIndividuals.Row propIndividualsRow = tuple._2();

                    Tuple2<IsEquivalentToSome.Row, String> mapping = fastLookup.getValue().get(prop);
                    IsEquivalentToSome.Row isEquivalentToSomeRow = mapping._1();
                    String individual = mapping._2();

                    return new Tuple2<>(individual, new Tuple2<>(isEquivalentToSomeRow, propIndividualsRow));
                });

        List<String> funtionalProperties = isFunctionalPropertyRDD.keys().distinct().collect();

        JavaPairRDD<String, String> finalRDD = secondJoinRDD
                // Filters out rows where individual == domain
                .filter(tuple -> {
                    String individual = tuple._1();
                    Tuple2<IsEquivalentToSome.Row, PropIndividuals.Row> secondOperand = tuple._2();

                    PropIndividuals.Row propIndividualsRow = secondOperand._2();

                    return individual.equals(propIndividualsRow.getDomain());
                })
                // Filters out only functional properties
                .filter(tuple -> {
                    Tuple2<IsEquivalentToSome.Row, PropIndividuals.Row> secondOperand = tuple._2();
                    PropIndividuals.Row propIndividualsRow = secondOperand._2();

                    return funtionalProperties.contains(propIndividualsRow.getProp());
                })
                .mapToPair(tuple -> {
                    Tuple2<IsEquivalentToSome.Row, PropIndividuals.Row> secondOperand = tuple._2();
                    IsEquivalentToSome.Row isEquivalentToSomeRow = secondOperand._1();
                    PropIndividuals.Row propIndividualsRow = secondOperand._2();
                    return new Tuple2<>(isEquivalentToSomeRow.getRange(), propIndividualsRow.getRange());
                });

        return finalRDD.collect();
    }

}
