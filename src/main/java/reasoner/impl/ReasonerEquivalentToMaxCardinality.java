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
import table.impl.IsEquivalentToMaxCardinality;
import table.impl.PropIndividuals;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToMaxCardinality extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToMaxCardinality.class);

    public ReasonerEquivalentToMaxCardinality(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToMaxCardinality(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for EquivalentToMaxCardinality");

        for (Tuple2<String, String> tuple : inferences) {
            String a1 = tuple._1();
            String a2 = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToSameAs(a1, a2, cache))
                    inferencesInserted++;
                // The inverse is also true
                if (insertToSameAs(a2, a1, cache))
                    inferencesInserted++;
            }
        }

        // We are potentially inserting (totalNumberInferences * 2) new inferences
        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences * 2);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsEquivalentToMaxCardinality.Row> isEquivalentToMaxCardinalityRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttomaxcardinality", CassandraJavaUtil.mapRowTo(IsEquivalentToMaxCardinality.Row.class))
                .keyBy((Function<IsEquivalentToMaxCardinality.Row, String>) IsEquivalentToMaxCardinality.Row::getCls)
                // Filters out rows where card === 1 and Clss === #Thing
                .filter(tuple -> {
                    IsEquivalentToMaxCardinality.Row row = tuple._2();
                    return row.getCard().equals("1") && row.getClss().equals("http://www.w3.org/2002/07/owl#Thing");
                });

        JavaPairRDD<String, PropIndividuals.Row> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp);

        JavaPairRDD<String, String> classIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getCls)
                .mapToPair(row -> {
                    String cls = row._1();
                    String individual = row._2().getIndividual();
                    return new Tuple2<>(cls, individual);
                });

        // Joins

        JavaPairRDD<String, String> firstJoinRDD = isEquivalentToMaxCardinalityRDD
                .join(classIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<IsEquivalentToMaxCardinality.Row, String> secondOperand = row._2();

                    IsEquivalentToMaxCardinality.Row isEquivalentToMaxCardinalityRow = secondOperand._1();
                    String individual = secondOperand._2();

                    return new Tuple2<>(isEquivalentToMaxCardinalityRow.getProp(), individual);
                });

        JavaPairRDD<String, PropIndividuals.Row> secondJoinRDD = firstJoinRDD
                .join(propIndividualsRDD)
                .mapToPair(row -> {
                    String prop = row._1();

                    Tuple2<String, PropIndividuals.Row> secondOperand = row._2();
                    PropIndividuals.Row propIndividualsRow = secondOperand._2();

                    return new Tuple2<>(prop, propIndividualsRow);
                });

        JavaPairRDD<String, String> thirdJoinRDD = secondJoinRDD
                .join(propIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<PropIndividuals.Row, PropIndividuals.Row> secondOperand = row._2();

                    PropIndividuals.Row propIndividuals1Row = secondOperand._1();
                    PropIndividuals.Row propIndividuals2Row = secondOperand._2();

                    return new Tuple2<>(propIndividuals1Row.getRange(), propIndividuals2Row.getRange());
                })
                .filter(tuple -> {
                    String range1 = tuple._1();
                    String range2 = tuple._2();

                    return !range1.equals(range2);
                });

        return thirdJoinRDD.collect();
    }

}
