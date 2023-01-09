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
import table.impl.IsObjectPropertyInverseOf;
import table.impl.PropIndividuals;

import java.util.List;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerObjectPropertyInverseOf extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerObjectPropertyInverseOf.class);

    public ReasonerObjectPropertyInverseOf(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerObjectPropertyInverseOf(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, Tuple2<String, String>>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for ObjectPropertyInverseOf");

        for (Tuple2<String, Tuple2<String, String>> tuple : inferences) {
            String subProp = tuple._1();

            Tuple2<String, String> secondOperand = tuple._2();
            String rangeProperty = secondOperand._1();
            String domainProperty = secondOperand._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToPropIndividuals(subProp, rangeProperty, domainProperty, cache))
                    inferencesInserted += 1;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, Tuple2<String, String>>> inference() {
        // Tables
        JavaPairRDD<String, IsObjectPropertyInverseOf.Row> isInverseOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isinverseof", CassandraJavaUtil.mapRowTo(IsObjectPropertyInverseOf.Row.class))
                .keyBy((Function<IsObjectPropertyInverseOf.Row, String>) IsObjectPropertyInverseOf.Row::getProp);

        if (LOGGER.isDebugEnabled())
            isInverseOfRDD.foreach(data -> {
                LOGGER.debug("iisinverseof prop=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        if (LOGGER.isDebugEnabled())
            propIndividualsRDD.foreach(data -> {
                LOGGER.debug("propindividuals prop=" + data._1() + " row=" + data._2());
            });

        List<String> keys = isInverseOfRDD.keys().distinct().collect();

        JavaPairRDD<String, PropIndividuals.Row> reducedRDD = propIndividualsRDD
                .filter(tuple -> keys.contains(tuple._1()));

        Broadcast<Map<String, IsObjectPropertyInverseOf.Row>> fastLookup = this.spark.broadcast(isInverseOfRDD.collectAsMap());

        JavaPairRDD<String, Tuple2<String, String>> firstJoinRDD = reducedRDD
                // Map-side join
                .mapToPair(tuple -> {
                    String prop = tuple._1();
                    PropIndividuals.Row propIndividualsRow = tuple._2();
                    IsObjectPropertyInverseOf.Row isInverseOfRow = fastLookup.getValue().get(prop);
                    return new Tuple2<>(isInverseOfRow.getInverse(), new Tuple2<>(propIndividualsRow.getRange(), propIndividualsRow.getDomain()));
                });

        return firstJoinRDD.collect();
    }

}
