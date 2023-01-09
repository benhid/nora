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
import table.impl.IsInverseFunctionalProperty;
import table.impl.PropIndividuals;

import java.util.ArrayList;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerInverseFunctionalProperty extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerInverseFunctionalProperty.class);

    public ReasonerInverseFunctionalProperty(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerInverseFunctionalProperty(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for InverseFunctionalProperty");

        for (Tuple2<String, String> tuple : inferences) {
            String domain1 = tuple._1();
            String domain2 = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToSameAs(domain1, domain2, cache))
                    inferencesInserted++;
                // The inverse is also true
                if (insertToSameAs(domain2, domain1, cache))
                    inferencesInserted++;
            }
        }

        // We are potentially inserting (totalNumberInferences * 2) new inferences
        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables
        JavaPairRDD<String, IsInverseFunctionalProperty.Row> isInverseFunctionalPropertyRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "isinversefunctionalproperty", CassandraJavaUtil.mapRowTo(IsInverseFunctionalProperty.Row.class))
                .keyBy((Function<IsInverseFunctionalProperty.Row, String>) IsInverseFunctionalProperty.Row::getProp);

        // If there are no functional properties, we can stop here
        // and avoid potentially expensive computations.
        if (isInverseFunctionalPropertyRDD.isEmpty()) {
            LOGGER.debug("No inverse functional properties found");
            return new ArrayList<>();
        }

        JavaPairRDD<String, Tuple2<String, String>> propIndividualsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getProp)
                .groupByKey()
                // Transform map <list of properties, individual> to
                //  multiple maps <property, 2-combination of domains>
                .flatMapToPair(row -> {
                    String prop = row._1();
                    Iterable<PropIndividuals.Row> individuals = row._2();

                    Tuple2<String, String> pair;
                    List<Tuple2<String, Tuple2<String, String>>> combinations = new ArrayList<>();

                    for (PropIndividuals.Row ind1 : individuals) {
                        for (PropIndividuals.Row ind2 : individuals) {
                            if (ind1.getRange().equals(ind2.getRange()) && !ind1.getDomain().equals(ind2.getDomain())) {
                                pair = new Tuple2<>(ind1.getDomain(), ind2.getDomain());
                                combinations.add(new Tuple2<>(prop, pair));
                            }
                        }
                    }

                    return combinations.iterator();
                });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = isInverseFunctionalPropertyRDD
                .join(propIndividualsRDD)
                .mapToPair(row -> {
                    Tuple2<IsInverseFunctionalProperty.Row, Tuple2<String, String>> tuple = row._2();
                    Tuple2<String, String> domains = tuple._2();

                    return new Tuple2<>(domains._1(), domains._2());
                });

        return firstJoinRDD.collect();
    }

}
