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
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import java.io.File;


public class LoadDBpedia {

    private final static Logger LOGGER = Logger.getLogger(LoadDBpedia.class);

    public static void main(String[] argv) {
        Configuration conf = new Configuration();
        Cassandra connection = new Cassandra(conf);

        try {
            connection.connect();
            connection.dropDatabaseIfExists();
            connection.createDatabaseIfNotExists();
        } catch (DBInitException e) {
            e.printStackTrace();
        }

        File ontology = new File("examples/DBpedia/dbpedia_2016-10.owl");

        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

        // Manually map imports.
        IRI documentIRI = IRI.create(ontology);
        IRI ontologyIRI = IRI.create("http://dbpedia.org/ontology/");

        SimpleIRIMapper mapper = new SimpleIRIMapper(ontologyIRI, documentIRI);
        manager.getIRIMappers().add(mapper);

        LoaderClassIndividuals loaderClassIndividuals = new LoaderClassIndividuals(connection, manager);
        LoaderPropIndividuals loaderPropIndividuals = new LoaderPropIndividuals(connection, manager);

        LOGGER.info("Loading instance types");

        File instanceTypes = new File("examples/DBpedia/instance-types_lang=en_2016-10.ttl");

        try {
            OWLOntology knowledgeGraph = manager.loadOntologyFromOntologyDocument(instanceTypes);

            loaderClassIndividuals
                    .loadOWLOntology(knowledgeGraph)
                    .forEach(connection::executeAsyncWithSession);
            //loaderPropIndividuals
            //        .loadOWLOntology(knowledgeGraph)
            //        .forEach(connection::executeAsyncWithSession);
        } catch (OWLOntologyCreationException e) {
            e.printStackTrace();
        }

        LOGGER.info("Loading mapping literals");

        File mappingBasedLiterals = new File("examples/DBpedia/mappingbased-literals_lang=en_2016-10.ttl");

        try {
            OWLOntology knowledgeGraph = manager.loadOntologyFromOntologyDocument(mappingBasedLiterals);

            loaderClassIndividuals
                    .loadOWLOntology(knowledgeGraph)
                    .forEach(connection::executeAsyncWithSession);
            loaderPropIndividuals
                    .loadOWLOntology(knowledgeGraph)
                    .forEach(connection::executeAsyncWithSession);
        } catch (OWLOntologyCreationException e) {
            e.printStackTrace();
        }

        LOGGER.info("Loading mapping objects ");

        File mappingBasedObjects = new File("examples/DBpedia/mappingbased_objects_wkd_uris_en.ttl");

        try {
            manager = OWLManager.createOWLOntologyManager();
            OWLOntology mappingObjects = manager.loadOntologyFromOntologyDocument(mappingBasedObjects);

            OpenlletReasoner reasoner = OpenlletReasonerFactory.getInstance().createReasoner(mappingObjects);

            new LoaderClassIndividuals(connection, manager)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderDisjointClasses(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsync);
            new LoaderEquivalentClasses(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderObjectPropertyDomains(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsync);
            new LoaderObjectPropertyEquivalentClasses(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderObjectPropertyInverses(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderObjectPropertyRanges(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsync);
            new LoaderObjectPropertySubProperties(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderPropIndividuals(connection, manager)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderSubClasses(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderSupClasses(connection, manager, reasoner)
                    .loadOWLOntology(mappingObjects)
                    .forEach(connection::executeAsync);
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOGGER.info("Loading ontology");

        try {
            manager = OWLManager.createOWLOntologyManager();
            OWLOntology TBox = manager.loadOntologyFromOntologyDocument(ontology);

            OpenlletReasoner reasoner = OpenlletReasonerFactory.getInstance().createReasoner(TBox);

            new LoaderClassIndividuals(connection, manager)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderDisjointClasses(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsync);
            new LoaderEquivalentClasses(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderObjectPropertyDomains(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsync);
            new LoaderObjectPropertyEquivalentClasses(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderObjectPropertyInverses(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderObjectPropertyRanges(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsync);
            new LoaderObjectPropertySubProperties(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderPropIndividuals(connection, manager)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderSubClasses(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsyncWithSession);
            new LoaderSupClasses(connection, manager, reasoner)
                    .loadOWLOntology(TBox)
                    .forEach(connection::executeAsync);
        } catch (Exception e) {
            e.printStackTrace();
        }

        connection.disconnect();
    }

}
