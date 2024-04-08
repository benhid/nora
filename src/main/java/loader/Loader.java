package loader;

import database.Configuration;
import database.DBInitException;
import database.impl.Cassandra;
import loader.impl.*;
import openllet.owlapi.OpenlletReasoner;
import openllet.owlapi.OpenlletReasonerFactory;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.parameters.Imports;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class Loader {

  private final static Logger LOGGER = Logger.getLogger(Loader.class);

  public static void main(String[] argv) {
    if (argv.length != 3) {
      System.err.println("Usage: Loader <ontology path> <individuals directory> <ontology IRI>");
      System.exit(1);
    }

    String ontologyArg = argv[0];
    String individualsArg = argv[1];
    String ontologyIRIArg = argv[2];

    Configuration conf = new Configuration();
    Cassandra connection = new Cassandra(conf);

    LOGGER.info("Connecting to Cassandra");

    try {
      connection.connect();
      LOGGER.info("Recreating keyspace");
      connection.dropDatabaseIfExists();
      connection.createDatabaseIfNotExists();
    } catch (DBInitException e) {
      LOGGER.error("An error occurred: " + e.getMessage(), e);
    }

    File ontology = new File(ontologyArg);

    OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

    // Manually map imports.
    IRI documentIRI = IRI.create(ontology);
    IRI ontologyIRI = IRI.create(ontologyIRIArg);

    SimpleIRIMapper mapper = new SimpleIRIMapper(ontologyIRI, documentIRI);
    manager.getIRIMappers().add(mapper);

    LoaderClassIndividuals loaderClassIndividuals = new LoaderClassIndividuals(connection, manager);
    LoaderPropIndividuals loaderPropIndividuals = new LoaderPropIndividuals(connection, manager);

    LOGGER.info("Loading individuals from " + individualsArg);

    try (Stream<Path> paths = Files.walk(Paths.get(individualsArg))) {
      OWLOntologyManager finalManager = manager;

      paths
          .filter(Files::isRegularFile)
          .map(Path::toFile)
          .forEach(individual -> {
            LOGGER.info(String.format("Found document %s", individual));

            try {
              LOGGER.info("Reading ontology from document");

              OWLOntology knowledgeGraph = finalManager.loadOntologyFromOntologyDocument(individual);

              LOGGER.info("Loaded!");

              loaderClassIndividuals
                  .loadOWLOntology(knowledgeGraph, Imports.EXCLUDED)
                  .forEach(connection::executeAsyncWithSession);
              loaderPropIndividuals
                  .loadOWLOntology(knowledgeGraph)
                  .forEach(connection::executeAsyncWithSession);
            } catch (OWLOntologyCreationException e) {
              e.printStackTrace();
            }
          });
    } catch (IOException e) {
      LOGGER.error("Failed to load individuals: " + e.getMessage(), e);
    }

    try {
      manager = OWLManager.createOWLOntologyManager();
      OWLOntology TBox = manager.loadOntologyFromOntologyDocument(ontology);

      OpenlletReasoner reasoner = OpenlletReasonerFactory.getInstance().createReasoner(TBox);

      loaderClassIndividuals
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      loaderPropIndividuals
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderDisjointClasses(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderEquivalentClasses(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderObjectPropertyDomains(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderObjectPropertyEquivalentClasses(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderObjectPropertyInverses(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderObjectPropertyRanges(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderObjectPropertySubProperties(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderSubClasses(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
      new LoaderSupClasses(connection, manager, reasoner)
          .loadOWLOntology(TBox)
          .forEach(connection::executeAsyncWithSession);
    } catch (Exception e) {
      e.printStackTrace();
    }

    LOGGER.info("Closing connection to Cassandra");

    connection.disconnect();
  }

}
