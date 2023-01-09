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
import table.impl.IsSameAs;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class ReasonerSameAsTransitiveClosure extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(ReasonerSameAsTransitiveClosure.class);

    public ReasonerSameAsTransitiveClosure(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public ReasonerSameAsTransitiveClosure(Database connection, JavaSparkContext sc, JedisPool pool) {
        super(connection, sc, pool);
    }

    public Integer resolve(List<Tuple2<String, String>> inferences) {
        int totalNumberInferences = inferences.size();
        int inferencesInserted = 0;

        LOGGER.info("Found " + totalNumberInferences + " inferences for SameAsClassIndividual");

        for (Tuple2<String, String> tuple : inferences) {
            String cls = tuple._1();
            String same = tuple._2();

            try (Jedis cache = this.pool.getResource()) {
                if (insertToClassIndividuals(cls, same, cache))
                    inferencesInserted++;
            }
        }

        LOGGER.info(inferencesInserted + " new inferences inserted out of " + totalNumberInferences);

        return inferencesInserted;
    }

    public List<Tuple2<String, String>> inference() {
        // Tables

        JavaPairRDD<String, IsSameAs.Row> isSameAsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "issameas", CassandraJavaUtil.mapRowTo(IsSameAs.Row.class))
                .keyBy((Function<IsSameAs.Row, String>) IsSameAs.Row::getInd);
        if (LOGGER.isDebugEnabled())
            isSameAsRDD.foreach(data -> {
                LOGGER.debug("issameas ind=" + data._1() + " row=" + data._2());
            });

        JavaPairRDD<String, String> closure = isSameAsRDD.mapToPair(row -> new Tuple2<>(row._1(), row._2().getSame()));

        // Linear transitive closure: each round grows paths by one edge,
        // by joining the graph's edges with the already-discovered paths.
        // e.g. join the path (y, z) from the TC with the edge (x, y) from
        // the graph to obtain the path (x, z).
        // see:
        // * http://www.java2s.com/example/java/big-data/transitive-closure-on-a-graph-via-apache-spark.html

        // Because join() joins on keys, the edges are stored in reversed order.
        JavaPairRDD<String, String> edges = closure.mapToPair(Tuple2::swap);

        edges.foreach(data -> {
            LOGGER.debug("issameas ind=" + data._1() + " row=" + data._2());
        });

        long oldCount;
        long newCount = closure.count();
        do {
            // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
            // then project the result to obtain the new (x, z) paths.
            JavaPairRDD<String, String> newEdges = closure.join(edges).mapToPair(row -> new Tuple2<>(row._2()._2(), row._2()._1()));
            JavaPairRDD<String, String> newClosure = closure.union(newEdges).distinct().cache();

            oldCount = newCount;
            newCount = newClosure.count();

            closure.unpersist();
            closure = newClosure;
        } while (newCount != oldCount);

        return closure.collect();
    }

}
