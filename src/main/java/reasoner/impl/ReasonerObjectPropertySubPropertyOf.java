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
import table.impl.IsSubPropertyOf;
import table.impl.PropIndividuals;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerObjectPropertySubPropertyOf extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerObjectPropertySubPropertyOf.class);

    public ReasonerObjectPropertySubPropertyOf(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerObjectPropertySubPropertyOf(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, Tuple2<String, String>>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for ObjectPropertySubProperty");

        for (Tuple2<String, Tuple2<String, String>> tuple : inferences) {
            String subProp = tuple._1();

            Tuple2<String, String> secondOperand = tuple._2();
            String domainProperty = secondOperand._1();
            String rangeProperty = secondOperand._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToPropIndividuals(subProp, domainProperty, rangeProperty, cache))
                    inferencesInserted += 1;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, Tuple2<String, String>>> inference() {
        // Tables

        JavaPairRDD<String, IsSubPropertyOf.Row> isSubPropertyOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issubpropertyof", CassandraJavaUtil.mapRowTo(IsSubPropertyOf.Row.class))
                .keyBy((Function<IsSubPropertyOf.Row, String>) IsSubPropertyOf.Row::getProp);

        if (LOGGER.isDebugEnabled())
            isSubPropertyOfRDD.foreach(data -> {
                LOGGER.debug("issubpropertyof prop=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        if (LOGGER.isDebugEnabled())
            propIndividualsRDD.foreach(data -> {
                LOGGER.debug("propindividuals prop=" + data._1() + " row=" + data._2());
            });

        // Joins

        JavaPairRDD<String, Tuple2<String, String>> firstJoinRDD = isSubPropertyOfRDD
                .join(propIndividualsRDD)
                .mapToPair(tuple -> {
                    Tuple2<IsSubPropertyOf.Row, PropIndividuals.Row> secondOperand = tuple._2();
                    IsSubPropertyOf.Row isSubPropertyOfRow = secondOperand._1();
                    PropIndividuals.Row propIndividualsRow = secondOperand._2();

                    return new Tuple2<>(isSubPropertyOfRow.getSubprop(), new Tuple2<>(propIndividualsRow.getDomain(), propIndividualsRow.getRange()));
                });

        return firstJoinRDD.collect();
    }

}
