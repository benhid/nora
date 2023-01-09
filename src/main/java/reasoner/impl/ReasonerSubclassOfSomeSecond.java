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
import table.impl.IsFunctionalProperty;
import table.impl.IsSubclassOfSome;
import table.impl.PropIndividuals;

import java.util.List;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerSubclassOfSomeSecond extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSubclassOfSomeSecond.class);

    public ReasonerSubclassOfSomeSecond(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSubclassOfSomeSecond(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SubclassOfSomeSecond");

        for (Tuple2<String, String> tuple : inferences) {
            String rangeIsSubclassOfSome = tuple._1();
            String rangePropIndividuals = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(rangeIsSubclassOfSome, rangePropIndividuals, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsSubclassOfSome.Row> isSubclassOfSomeRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issubclassofsome", CassandraJavaUtil.mapRowTo(IsSubclassOfSome.Row.class))
                .keyBy((Function<IsSubclassOfSome.Row, String>) IsSubclassOfSome.Row::getCls);

        if (LOGGER.isDebugEnabled())
            isSubclassOfSomeRDD.foreach(data -> {
                LOGGER.debug("issubclassofsome cls=" + data._1() + " row=" + data._2());
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

        JavaPairRDD<String, Tuple2<IsSubclassOfSome.Row, String>> firstJoinRDD = isSubclassOfSomeRDD
                .join(classIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsSubclassOfSome.Row, String> secondOperand = tuple._2();
                    IsSubclassOfSome.Row isSubclassOfSomeRow = secondOperand._1();
                    String individual = secondOperand._2();

                    return new Tuple2<>(isSubclassOfSomeRow.getProp(), new Tuple2<>(isSubclassOfSomeRow, individual));
                });

        // In general, since our data are distributed among many Spark nodes, they have to be shuffled before a 
        // join causing significant network I/O and slow performance.
        // This is the case with 'propindividuals', as it usually is a very large RDD.

        // The following code is an attempt to optimize the join by reducing the size of the large RDD
        // before doing the shuffle. It is important to note that the efficiency gain here depends on the filter
        // operation actually reducing the size of the larger RDD. If there are not a lot of entries lost here
        // (e.g., because the medium size RDD is some king of large dimension table), there is nothing to be gained
        // with this strategy.
        List<String> keys = firstJoinRDD.keys().distinct().collect();

        JavaPairRDD<String, PropIndividuals.Row> reducedRDD = propIndividualsRDD
                .filter(tuple -> keys.contains(tuple._1()));

        // The reduced RDD is now smaller, but the join might still be slow.
        // We can turn the small RDD into a broadcast variable and turn the entire operation into a so called
        // map side join for the larger RDD (now reduced).
        // In this way the larger RDD does not need to be shuffled at all:
        Broadcast<Map<String, Tuple2<IsSubclassOfSome.Row, String>>> fastLookup = this.spark.broadcast(firstJoinRDD.collectAsMap());

        JavaPairRDD<String, Tuple2<IsSubclassOfSome.Row, PropIndividuals.Row>> secondJoinRDD = reducedRDD
                // Map-side join
                .mapToPair(tuple -> {
                    String prop = tuple._1();
                    PropIndividuals.Row propIndividualsRow = tuple._2();

                    Tuple2<IsSubclassOfSome.Row, String> mapping = fastLookup.getValue().get(prop);
                    IsSubclassOfSome.Row isSubclassOfSomeRow = mapping._1();
                    String individual = mapping._2();

                    return new Tuple2<>(individual, new Tuple2<>(isSubclassOfSomeRow, propIndividualsRow));
                });

        // TODO - Use a broadcast variable for the functional properties as well.
        List<String> funtionalProperties = isFunctionalPropertyRDD.keys().distinct().collect();

        JavaPairRDD<String, String> thirdJoinRDD = secondJoinRDD
                // Filters out rows where individual == domain
                .filter(tuple -> {
                    String individual = tuple._1();
                    Tuple2<IsSubclassOfSome.Row, PropIndividuals.Row> secondOperand = tuple._2();

                    PropIndividuals.Row propIndividualsRow = secondOperand._2();

                    return individual.equals(propIndividualsRow.getDomain());
                })
                // Filters out only functional properties
                .filter(tuple -> {
                    Tuple2<IsSubclassOfSome.Row, PropIndividuals.Row> secondOperand = tuple._2();
                    PropIndividuals.Row propIndividualsRow = secondOperand._2();

                    return funtionalProperties.contains(propIndividualsRow.getProp());
                })
                .mapToPair(tuple -> {
                    Tuple2<IsSubclassOfSome.Row, PropIndividuals.Row> secondOperand = tuple._2();
                    IsSubclassOfSome.Row isSubclassOfSomeRow = secondOperand._1();
                    PropIndividuals.Row propIndividualsRow = secondOperand._2();
                    return new Tuple2<>(isSubclassOfSomeRow.getRange(), propIndividualsRow.getRange());
                });

        return thirdJoinRDD.collect();
    }

}
