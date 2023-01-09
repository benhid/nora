package reasoner;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import database.Configuration;
import database.DBInitException;
import database.Database;
import database.impl.Cassandra;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import scala.Tuple2;
import table.impl.ClassIndividuals;
import table.impl.PropIndividuals;

import java.time.Duration;
import java.time.Instant;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

public class QueryResultSetUOBM extends ReasonerManager {

    private final static Logger LOGGER = Logger.getLogger(QueryResultSetUOBM.class);

    public QueryResultSetUOBM(Database connection, SparkConf conf, JedisPool pool) {
        super(connection, conf, pool);
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();

        Cassandra connection = new Cassandra(conf);
        JedisPool pool = new JedisPool(new JedisPoolConfig(), conf.getProperty("redis_host"), Integer.parseInt(conf.getProperty("redis_port")));

        SparkConf sparkConf = new SparkConf()
                .setAppName("QueryResultSetUOBM")
                .setMaster("local[*]")
                .set("spark.cassandra.connection.host", connection.getHost())
                .set("spark.cassandra.connection.port", String.valueOf(connection.getPort()))
                .set("spark.cassandra.auth.username", connection.getUsername())
                .set("spark.cassandra.auth.password", connection.getPassword())
                .set("spark.driver.maxResultSize", "0")
                .set("spark.driver.allowMultipleContexts", "true");

        try (Jedis cache = pool.getResource()) {
            LOGGER.info("Flushing cache");
            cache.flushAll();
        }

        try {
            connection.connect();

            // Make inferences
            QueryResultSetUOBM uobm = new QueryResultSetUOBM(connection, sparkConf, pool);
            Instant start = Instant.now();
            uobm.Q1(); // 32
            LOGGER.info("'Q1' took " + Duration.between(start, Instant.now()).getSeconds() % 60 + " milliseconds to run");
            start = Instant.now();
            uobm.Q2(); // 2511
            LOGGER.info("'Q2' took " + Duration.between(start, Instant.now()).getSeconds() % 60 + " milliseconds to run");
            start = Instant.now();
            uobm.Q3(); // 666
            LOGGER.info("'Q3' took " + Duration.between(start, Instant.now()).getSeconds() % 60 + " milliseconds to run");
            start = Instant.now();
            uobm.Q4(); // 405
            LOGGER.info("'Q4' took " + Duration.between(start, Instant.now()).getSeconds() % 60 + " milliseconds to run");
            start = Instant.now();
            uobm.Q6(); // 170
            LOGGER.info("'Q6' took " + Duration.between(start, Instant.now()).getSeconds() % 60 + " milliseconds to run");
            start = Instant.now();
            uobm.Q8(); // (don't work)
            LOGGER.info("'Q8' took " + Duration.between(start, Instant.now()).getSeconds() % 60 + " milliseconds to run");
            start = Instant.now();
            uobm.Q9(); // 1055 (don't work)
            LOGGER.info("'Q9' took " + Duration.between(start, Instant.now()).getSeconds() % 60 + " milliseconds to run");
            start = Instant.now();
            uobm.Q12();// 57
            LOGGER.info("'Q12' took " + Duration.between(start, Instant.now()).getSeconds() % 60 + " milliseconds to run");
        } catch (DBInitException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            connection.disconnect();
        }
    }

    public void Q1() {
        // Tables
        JavaPairRDD<String, String> undergradsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .mapToPair(row -> {
                    String individual = row._1();
                    String cls = row._2().getCls();

                    return new Tuple2<>(individual, cls);
                })
                // Filters out rows where cls === #UndergraduateStudent
                .filter(tuple -> {
                    String cls = tuple._2();
                    return cls.equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#UndergraduateStudent");
                });

        JavaPairRDD<String, String> takesCourseRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getDomain)
                // Filters out rows where prop === #publicationAuthor
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#takesCourse")
                            && prop.getRange().equals("http://www.Department0.University0.edu/Course0");
                })
                .mapToPair(row -> {
                    String individual = row._1(); // domain
                    String course = row._2().getRange();  // range

                    return new Tuple2<>(individual, course);
                });

        // Joins
        JavaPairRDD<String, Tuple2<String, String>> firstJoinRDD = undergradsRDD
                .join(takesCourseRDD);

        firstJoinRDD
                .map(Tuple2::_1)
                .distinct()
                .collect();
        //.coalesce(1)
        //.saveAsTextFile(new File("lite-1-q-1").getAbsolutePath());
    }

    public void Q2() {
        // Tables
        JavaPairRDD<String, String> employeesRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .mapToPair(row -> {
                    String individual = row._1();
                    String cls = row._2().getCls();

                    return new Tuple2<>(individual, cls);
                })
                // Filters out rows where cls === #Employee
                .filter(tuple -> {
                    String cls = tuple._2();
                    return cls.equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#Employee");
                });

        // Persist
        employeesRDD
                .map(Tuple2::_1)
                .distinct()
                .collect();
        //.coalesce(1)
        //.saveAsTextFile(new File("lite-1-q-2").getAbsolutePath());
    }

    public void Q3() {
        // Tables
        JavaPairRDD<String, String> studentsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .mapToPair(row -> {
                    String individual = row._1();
                    String cls = row._2().getCls();

                    return new Tuple2<>(individual, cls);
                })
                // Filters out rows where cls === #Student
                .filter(tuple -> {
                    String cls = tuple._2();
                    return cls.equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#Student");
                });

        JavaPairRDD<String, String> isMemberOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getDomain)
                // Filters out rows where prop === #isMemberOf
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#isMemberOf")
                            && prop.getRange().equals("http://www.Department0.University0.edu");
                })
                .mapToPair(row -> {
                    String person = row._1();
                    String university = row._2().getRange();

                    return new Tuple2<>(person, university);
                });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = studentsRDD
                .join(isMemberOfRDD)
                .mapToPair(row -> {
                    String student = row._1();

                    Tuple2<String, String> secondOperand = row._2();
                    String university = secondOperand._2();

                    return new Tuple2<>(student, university);
                });

        // Persist
        firstJoinRDD
                .map(Tuple2::_1)
                .distinct()
                .collect();
        //.coalesce(1)
        //.saveAsTextFile(new File("lite-1-q-3").getAbsolutePath());
    }

    public void Q4() {
        // Tables
        JavaPairRDD<String, String> publicationsRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .mapToPair(row -> {
                    String individual = row._1();
                    String cls = row._2().getCls();

                    return new Tuple2<>(individual, cls);
                })
                // Filters out rows where cls === #Publication
                .filter(tuple -> {
                    String cls = tuple._2();
                    return cls.equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#Publication");
                });

        JavaPairRDD<String, String> publicationAuthorRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getDomain)
                // Filters out rows where prop === #publicationAuthor
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#publicationAuthor");
                })
                .mapToPair(row -> {
                    String individual = row._1(); // domain
                    String author = row._2().getRange();  // range

                    return new Tuple2<>(individual, author);
                });

        JavaPairRDD<String, String> facultyRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .mapToPair(row -> {
                    String individual = row._1();
                    String cls = row._2().getCls();

                    return new Tuple2<>(individual, cls);
                })
                // Filters out rows where cls === #Faculty
                .filter(tuple -> {
                    String cls = tuple._2();
                    return cls.equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#Faculty");
                });

        JavaPairRDD<String, String> isMemberOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getDomain)
                // Filters out rows where prop === #isMemberOf
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#isMemberOf")
                            && prop.getRange().equals("http://www.Department0.University0.edu");
                })
                .mapToPair(row -> {
                    String individual = row._1(); // domain
                    String org = row._2().getRange();  // range

                    return new Tuple2<>(individual, org);
                });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = publicationsRDD
                .join(publicationAuthorRDD)
                .mapToPair(row -> {
                    String publication = row._1();
                    String author = row._2()._2();

                    return new Tuple2<>(author, publication);
                });

        JavaPairRDD<String, String> secondJoinRDD = facultyRDD
                .join(isMemberOfRDD)
                .mapToPair(row -> {
                    String faculty = row._1();
                    String org = row._2()._2();

                    return new Tuple2<>(faculty, org);
                });

        JavaPairRDD<String, String> thirdJoinRDD = firstJoinRDD
                .join(secondJoinRDD)
                .mapToPair(row -> {
                    String publication = row._2()._1();
                    String org = row._2()._2();

                    return new Tuple2<>(publication, org);
                });

        // Persist
        thirdJoinRDD
                .map(Tuple2::_1)
                .distinct()
                .collect();
        //.coalesce(1)
        //.saveAsTextFile(new File("lite-1-q-4").getAbsolutePath());
    }

    public void Q6() {
        // Tables
        JavaPairRDD<String, String> peopleRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .mapToPair(row -> {
                    String individual = row._1();
                    String cls = row._2().getCls();

                    return new Tuple2<>(individual, cls);
                })
                // Filters out rows where cls === #Person
                .filter(tuple -> {
                    String cls = tuple._2();
                    return cls.equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#Person");
                });

        JavaPairRDD<String, String> universityRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getRange)
                // Filters out rows where prop === #hasAlumnus
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#hasAlumnus")
                            && prop.getDomain().equals("http://www.University0.edu");
                })
                .mapToPair(row -> {
                    String student = row._1();
                    String university = row._2().getRange();

                    return new Tuple2<>(student, university);
                });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = peopleRDD
                .join(universityRDD)
                .mapToPair(row -> {
                    String student = row._1();

                    Tuple2<String, String> secondOperand = row._2();
                    String university = secondOperand._2();

                    return new Tuple2<>(student, university);
                });

        // Persist
        firstJoinRDD
                .map(Tuple2::_1)
                .distinct()
                .collect();
        //.coalesce(1)
        //.saveAsTextFile(new File("lite-1-q-6").getAbsolutePath());
    }

    public void Q8() {
        // todo http://www.Department0.University0.edu/UndergraduateStudent90 love Basketball
        // todo http://uob.iodt.ibm.com/univ-bench-lite.owl#love is equivalent to #like : http://www.Department0.University0.edu/UndergraduateStudent90
        // todo ^^^ NOT LOADING EQUIVALENT OBJECT PROPERTIES!

        // Tables
        JavaPairRDD<String, String> sportsLoversRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .mapToPair(row -> {
                    String individual = row._1();
                    String cls = row._2().getCls();

                    return new Tuple2<>(individual, cls);
                })
                // Filters out rows where cls === #SportsLover
                .filter(tuple -> {
                    String cls = tuple._2();
                    return cls.equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#SportsLover");
                });

        JavaPairRDD<String, String> universityRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getRange)
                // Filters out rows where prop === #hasMember
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#hasMember")
                            && prop.getDomain().equals("http://www.Department0.University0.edu");
                })
                .mapToPair(row -> {
                    String member = row._1();
                    String department = row._2().getRange();

                    return new Tuple2<>(member, department);
                });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = sportsLoversRDD
                .join(universityRDD)
                .mapToPair(row -> {
                    String member = row._1();

                    Tuple2<String, String> secondOperand = row._2();
                    String department = secondOperand._2();

                    return new Tuple2<>(member, department);
                });

        // Persist
        firstJoinRDD
                .map(Tuple2::_1)
                .distinct()
                .collect();
        //.coalesce(1)
        //.saveAsTextFile(new File("lite-1-q-8").getAbsolutePath());
    }

    public void Q9() {
        // Tables
        JavaPairRDD<String, String> graduateCoursesRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "classindividuals", CassandraJavaUtil.mapRowTo(ClassIndividuals.Row.class))
                .keyBy((Function<ClassIndividuals.Row, String>) ClassIndividuals.Row::getIndividual)
                .mapToPair(row -> {
                    String individual = row._1();
                    String cls = row._2().getCls();

                    return new Tuple2<>(individual, cls);
                })
                // Filters out rows where cls === #GraduateCourse
                .filter(tuple -> {
                    String cls = tuple._2();
                    return cls.equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#GraduateCourse");
                });

        JavaPairRDD<String, String> isTaughtByRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getDomain)
                // Filters out rows where prop === #isTaughtBy
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#isTaughtBy");
                })
                .mapToPair(row -> {
                    String course = row._1();
                    String professor = row._2().getRange();

                    return new Tuple2<>(course, professor);
                });

        JavaPairRDD<String, String> isMemberOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getDomain)
                // Filters out rows where prop === #isMemberOf
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#isMemberOf");
                })
                .mapToPair(row -> {
                    String person = row._1();
                    String department = row._2().getRange();

                    return new Tuple2<>(department, person);
                });

        JavaPairRDD<String, String> subOrganizationOfRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getRange)
                // Filters out rows where prop === #subOrganizationOf
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#subOrganizationOf")
                            && prop.getDomain().equals("http://www.University0.edu");
                })
                .mapToPair(row -> {
                    String department = row._1();
                    String university = row._2().getRange(); // always the same

                    return new Tuple2<>(department, university);
                });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = graduateCoursesRDD
                .join(isTaughtByRDD)
                .mapToPair(row -> {
                    String course = row._1();

                    Tuple2<String, String> secondOperand = row._2();
                    String professor = secondOperand._2();

                    return new Tuple2<>(professor, course);
                });

        JavaPairRDD<String, String> secondJoinRDD = isMemberOfRDD
                .join(subOrganizationOfRDD)
                .mapToPair(row -> {
                    Tuple2<String, String> secondOperand = row._2();
                    String person = secondOperand._1();
                    String university = secondOperand._2();

                    return new Tuple2<>(person, university);
                });

        JavaPairRDD<String, String> thirdJoinRDD = firstJoinRDD
                .join(secondJoinRDD)
                .mapToPair(row -> {
                    Tuple2<String, String> secondOperand = row._2();
                    String course = secondOperand._1();
                    String university = secondOperand._2();

                    return new Tuple2<>(course, university);
                });

        thirdJoinRDD.foreach(t -> System.out.println("third " + t._1 + " " + t._2));

        // Persist
        thirdJoinRDD
                .map(Tuple2::_1)
                .distinct()
                .collect();
        //.coalesce(1)
        //.saveAsTextFile(new File("lite-1-q-9").getAbsolutePath());
    }

    public void Q12() {
        // Tables
        JavaPairRDD<String, String> takesCourseRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getDomain)
                // Filters out rows where prop === #takesCourse
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#takesCourse");
                })
                .mapToPair(row -> {
                    String student = row._1();
                    String course = row._2().getRange();

                    return new Tuple2<>(course, student);
                });

        JavaPairRDD<String, String> isTaughtByRDD = javaFunctions(spark)
                .cassandraTable(connection.getDatabaseName(), "propindividuals", CassandraJavaUtil.mapRowTo(PropIndividuals.Row.class))
                .keyBy((Function<PropIndividuals.Row, String>) PropIndividuals.Row::getDomain)
                // Filters out rows where prop === #isTaughtBy
                .filter(tuple -> {
                    PropIndividuals.Row prop = tuple._2();
                    return prop.getProp().equals("http://uob.iodt.ibm.com/univ-bench-lite.owl#isTaughtBy")
                            && prop.getRange().equals("http://www.Department0.University0.edu/FullProfessor0");
                })
                .mapToPair(row -> {
                    String course = row._1();
                    String professor = row._2().getRange();

                    return new Tuple2<>(course, professor);
                });

        // Joins
        JavaPairRDD<String, String> firstJoinRDD = takesCourseRDD
                .join(isTaughtByRDD)
                .mapToPair(row -> {
                    Tuple2<String, String> secondOperand = row._2();
                    String student = secondOperand._1();
                    String professor = secondOperand._2();

                    return new Tuple2<>(student, professor);
                });

        // Persist
        firstJoinRDD
                .map(Tuple2::_1)
                .distinct()
                .collect();
        //.coalesce(1)
        //.saveAsTextFile(new File("lite-1-q-12").getAbsolutePath());
    }

}
