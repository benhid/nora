package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(MockitoExtension.class)
public class LoaderPropIndividualsTest {

    final File ontology = new File("src/test/resources/LoaderPropIndividuals/ontology.owl");
    final File individual = new File("src/test/resources/LoaderPropIndividuals/individual.owl");


    @Mock
    private Database mockedDatabase;

    @BeforeEach
    void setup() {
        Mockito.lenient().when(mockedDatabase.getDatabaseName()).thenReturn("test");
    }

    @Test
    void shouldReturnPropIndividuals() throws OWLOntologyCreationException {
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

        // Manually map imports
        IRI documentIRI = IRI.create(ontology);
        IRI ontologyIRI = IRI.create("http://www.semanticweb.org/mmar/ontologies/2020/4/untitled-ontology-33");

        SimpleIRIMapper mapper = new SimpleIRIMapper(ontologyIRI, documentIRI);
        manager.getIRIMappers().add(mapper);

        OWLOntology knowledgeGraph = manager.loadOntologyFromOntologyDocument(individual);

        List<SimpleStatement> results = new LoaderPropIndividuals(mockedDatabase, manager)
                .loadOWLOntology(knowledgeGraph)
                .collect(Collectors.toList());

        assertEquals(4, results.size());
    }

}