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
import table.impl.IsEquivalentToIntersection;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerEquivalentToIntersectionSecond extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerEquivalentToIntersectionSecond.class);

    public ReasonerEquivalentToIntersectionSecond(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerEquivalentToIntersectionSecond(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for EquivalentToIntersectionSecond");

        for (Tuple2<String, String> tuple : inferences) {
            String cls = tuple._1();
            String individual = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(cls, individual, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables
        JavaPairRDD<TreeSet<String>, String> isEquivalentToIntersectionRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isequivalenttointersection", CassandraJavaUtil.mapRowTo(IsEquivalentToIntersection.Row.class))
                .mapToPair(row -> {
                    // Keeping order is important, as we'll join this table with other by a list of values,
                    // and we need to be sure that lists are sorted for comparison.
                    // The most straightforward way is using a treeset, with sorts by natural order*.
                    // * https://stackoverflow.com/questions/8725387/why-is-there-no-sortedlist-in-java
                    TreeSet<String> ind = new TreeSet<>();
                    ind.add(row.getInd1());
                    ind.add(row.getInd2());

                    return new Tuple2<>(ind, row.getCls());
                });

        if (LOGGER.isDebugEnabled())
            isEquivalentToIntersectionRDD.foreach(data -> {
                LOGGER.debug("isequivalenttointersection individuals=" + data._1() + " cls=" + data._2());
            });

        JavaPairRDD<TreeSet<String>, String> individualsByClassRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .groupByKey()
                .mapToPair(row -> {
                    String cls = row._1();
                    List<String> classes = new ArrayList<>();

                    for (ClassIndividuals.Row innerRow : row._2()) {
                        classes.add(innerRow.getCls());
                    }

                    return new Tuple2<>(classes, cls);
                })
                // Transform map <list of classes, individual> to
                //  multiple maps <2-combination of classes, individual>
                .flatMapToPair(row -> {
                    String individual = row._2();
                    List<String> classes = row._1();

                    // Just as before, keeping order is important to compare list of pairs -> treeset provides natural
                    // order by default.
                    List<Tuple2<TreeSet<String>, String>> combinations = new ArrayList<>();

                    for (int i = 0; i < classes.size(); i++) {
                        for (int j = i + 1; j < classes.size(); j++) {
                            TreeSet<String> pair = new TreeSet<>();
                            pair.add(classes.get(i));
                            pair.add(classes.get(j));

                            combinations.add(new Tuple2<>(pair, individual));
                        }
                    }

                    return combinations.iterator();
                });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = isEquivalentToIntersectionRDD
                .join(individualsByClassRDD)
                .mapToPair(Tuple2::_2);

        return firstJoinRDD.collect();
    }

}
