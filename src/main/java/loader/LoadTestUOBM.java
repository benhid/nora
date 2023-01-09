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


public class LoadTestUOBM {

    private final static Logger LOGGER = Logger.getLogger(LoadTestUOBM.class);

    public static void main(String[] argv) {
        Configuration conf = new Configuration();
        Cassandra connection = new Cassandra(conf);

        LOGGER.info("Connecting to Cassandra");

        try {
            connection.connect();

            LOGGER.info("Dropping keyspace");

            connection.dropDatabaseIfExists();

            LOGGER.info("Recreating keyspace");

            connection.createDatabaseIfNotExists();
        } catch (DBInitException e) {
            e.printStackTrace();
        }

        File ontology = new File("examples/TestUOBM/ontology.owl");

        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

        // Manually map imports
        IRI documentIRI = IRI.create(ontology);
        IRI ontologyIRI = IRI.create("http://uob.iodt.ibm.com/univ-bench-lite.owl");

        SimpleIRIMapper mapper = new SimpleIRIMapper(ontologyIRI, documentIRI);
        manager.getIRIMappers().add(mapper);

        LoaderClassIndividuals loaderClassIndividuals = new LoaderClassIndividuals(connection, manager);
        LoaderPropIndividuals loaderPropIndividuals = new LoaderPropIndividuals(connection, manager);

        try (Stream<Path> paths = Files.walk(Paths.get("examples/TestUOBM/individuals/"))) {
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
            e.printStackTrace();
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

        LOGGER.info("Completed");

        connection.disconnect();
    }

}
