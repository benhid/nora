# NORA: Scalable OWL reasoner based on NoSQL databases and Apache Spark

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Tested with [OpenJDK 11](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/generic-linux-install.html) and Gradle 7.5.1.

## Building the project

Use `gradle` to build the project:

```bash
$ make build
```

## Getting started

Before running the project, edit the `config.properties` and set your own values. Make sure to set the `NORA_CONFIG` environment variable to the path of the `config.properties` file:

```bash
$ export NORA_CONFIG=$(pwd)/config.properties
```

Load the ontology into the database:

```bash
$ java -cp nora.jar loader.Loader examples/ontology.owl examples/individuals/ http://w3id.org/examples/ontology.owl
```

Then, start the reasoning process by running the following command:

```bash
$ java -cp nora.jar reasoner.Reasoner
```

or by using `spark-submit`:

```bash
$ spark-submit \
    --name nora \
    --class reasoner.Reasoner \
    --master spark://host:port \
    --driver-memory 6G \
    --executor-memory 120G \
    file:///home/user/nora.jar 10
```

A step-by-step guide is available in the [docs](docs/README.md) directory.

## Troubleshooting

#### Exception `java.lang.OutOfMemoryError: Java heap space` when running Apache Spark in client mode 

The flag `-Xmx` specifies the maximum memory allocation pool for a Java Virtual Machine (JVM), while `-Xms` specifies the initial memory allocation pool. These flags can be set to modify the Java memory settings:

- IntelliJ IDEA: Run/Debug configuration > Set VM options to `-Xms10g -Xmx80g`
- Terminal: Run Java with `-Xms10g -Xmx80g`

## Tests

Run the following command to run the tests:

```console
$ make test
```

Embedded Cassandra requires JDK 11 or lower. If using a higher version, downgrade it to JDK 11.

## License

This project is licensed under the MIT license - see the [LICENSE](LICENSE) file for details.