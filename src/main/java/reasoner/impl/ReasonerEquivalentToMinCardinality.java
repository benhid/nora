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
import table.impl.IsEquivalentToMinCardinality;
import table.impl.PropIndividuals;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToMinCardinality extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToMinCardinality.class);

    public ReasonerEquivalentToMinCardinality(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToMinCardinality(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for MinCardinality");

        for (Tuple2<String, String> tuple : inferences) {
            String cls = tuple._1();
            String domain = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(cls, domain, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables
        JavaPairRDD<String, IsEquivalentToMinCardinality.Row> isEquivalentToMinCardinalityRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttomincardinality", CassandraJavaUtil.mapRowTo(IsEquivalentToMinCardinality.Row.class))
                .keyBy((Function<IsEquivalentToMinCardinality.Row, String>) IsEquivalentToMinCardinality.Row::getProp)
                // Filters out rows where card === 1 and Clss === #Thing
                .filter(tuple -> {
                    IsEquivalentToMinCardinality.Row row = tuple._2();
                    return row.getCard().equals("1") && row.getClss().equals("http://www.w3.org/2002/07/owl#Thing");
                });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = isEquivalentToMinCardinalityRDD
                .join(propIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<IsEquivalentToMinCardinality.Row, PropIndividuals.Row> tuple = row._2();

                    IsEquivalentToMinCardinality.Row isEquivalentToMinCardinalityRow = tuple._1();
                    PropIndividuals.Row propIndividuals = tuple._2();

                    return new Tuple2<>(isEquivalentToMinCardinalityRow.getCls(), propIndividuals.getDomain());
                });

        return firstJoinRDD.collect();
    }

}
