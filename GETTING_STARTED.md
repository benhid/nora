## Project Structure

The project is divided into two main modules:

* The [`loader`](/src/main/java/loader) module is responsible for loading the knowledge base and storing it in Apache Cassandra. The input files are in the form of OWL and RDF files, which are [parsed](/src/main/java/loader/impl) with the help of the OWL API. The database schema is designed to store the data in a way that is efficient for querying (for further details, check the [implementations](/src/main/java/table/impl/)).

* The [`reasoner`](/src/main/java/reasoner) module reads the data from Apache Cassandra using Apache Spark and performs the reasoning tasks (transformations) on the data by running [Spark jobs](/src/main/java/reasoner/impl) sequentially Inferences, i.e., new derived knowledge from the knowledge base, are then stored back in Apache Cassandra. This loop continues until no new inferences are generated.

## Getting Started

The following sections provide a step-by-step guide to building and running the project using the LUBM knowledge base, a synthetic dataset for the university domain. Before moving, make sure you have the necessary software installed:

* Apache Spark
* Java 11
* [Gradle](https://gradle.org/install/)
* [Docker](https://docs.docker.com/get-docker/)
* Maven (for generating the test data)

### Pre-requisites

1. Download the [LUBM ontology](https://swat.cse.lehigh.edu/projects/lubm/) (core ontology):

```bash
wget http://swat.cse.lehigh.edu/onto/univ-bench.owl
```

2. Clone the [UBA data generator](https://github.com/rvesse/lubm-uba) repository and compile the artifacts using Maven:

```bash
git clone https://github.com/rvesse/lubm-uba.git
cd lubm-uba/
mvn clean install
```

3. Produce test data:

```bash
mkdir individuals/
java -jar uba-1.8.jar \
  -u 1 \
  -o individuals/ \
  -t 8 \
  --onto "http://swat.cse.lehigh.edu/onto/univ-bench.owl" \
  --format NTRIPLES \
  --consolidate Maximal
```

### Reasoning on the LUBM Knowledge Base

1. Clone the repository:

```bash
git clone https://github.com/benhid/nora.git
cd nora/
```

2. Build the project using `gradle`:

```bash
make build
```

3. Launch the Apache Cassandra and Redis servers:

> Here we are using ScyllaDB and Dragonfly as drop-in replacements for Apache Cassandra and Redis, respectively. You can use the official images for Apache Cassandra and Redis if you prefer.

```bash
docker run --name scylla-master -p 7000:7000 -p 7001:7001 -p 9042:9042 -p 9160:9160 -p 10000 -d scylladb/scylla
docker run --name dragonfly-master -p 6379:6379 -d docker.dragonflydb.io/dragonflydb/dragonfly
```

4. Before running the loader, create a `config.properties` file and set the `NORA_CONFIG` environment variable to its path:

```bash
cat >config.properties <<EOL
cassandra_host=localhost
cassandra_port=9042
cassandra_username=cassandra
cassandra_password=cassandra
cassandra_database=univbench
redis_host=localhost
redis_port=6379
EOL

export NORA_CONFIG=$(pwd)/config.properties
```

If you are using a different configuration, make sure to update the `config.properties` file accordingly. For instance, if you use an external Spark cluster, you must set the `spark_host` property to the Spark master URL.

5. Finally, run the loader:

```bash
java -cp nora.jar loader.Loader univ-bench.owl individuals/ http://swat.cse.lehigh.edu/onto/univ-bench.owl
```

> Check the implementation of the loader to understand how the data is loaded into Apache Cassandra. For specific requirements, you may need to modify the loader implementation (e.g., to load data from a different source or in a different format).

6. Once the loader has finished, you can run the reasoner:

```bash
java -cp nora.jar reasoner.Reasoner
```

> Similarly, the reasoner implementation contains the logic for performing the reasoning tasks on the knowledge base. You can extend the reasoner to add new tasks to the loop or modify the existing ones.
