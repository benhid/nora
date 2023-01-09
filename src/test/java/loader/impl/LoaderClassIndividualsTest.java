package loader.impl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
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
import org.semanticweb.owlapi.model.parameters.Imports;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(MockitoExtension.class)
public class LoaderClassIndividualsTest {

    final File ontology = new File("src/test/resources/LoaderClassIndividuals/ontology.owl");
    final File individual = new File("src/test/resources/LoaderClassIndividuals/individual.owl");


    @Mock
    private Database mockedDatabase;

    @BeforeEach
    void setup() {
        Mockito.lenient().when(mockedDatabase.getDatabaseName()).thenReturn("test");
    }

    @Test
    void shouldReturnClassIndividuals() throws OWLOntologyCreationException {
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

        // Manually map imports
        IRI documentIRI = IRI.create(ontology);
        IRI ontologyIRI = IRI.create("http://www.semanticweb.org/mmar/ontologies/2020/4/untitled-ontology-33");

        SimpleIRIMapper mapper = new SimpleIRIMapper(ontologyIRI, documentIRI);
        manager.getIRIMappers().add(mapper);

        OWLOntology knowledgeGraph = manager.loadOntologyFromOntologyDocument(individual);

        List<SimpleStatement> results = new LoaderClassIndividuals(mockedDatabase, manager)
                .loadOWLOntology(knowledgeGraph, Imports.EXCLUDED)
                .collect(Collectors.toList());

        assertEquals(2, results.size());

        results = new LoaderClassIndividuals(mockedDatabase, manager)
                .loadOWLOntology(knowledgeGraph, Imports.INCLUDED)
                .collect(Collectors.toList());

        assertEquals(3, results.size());

        String cls = "http://www.semanticweb.org/mmar/ontologies/2020/3/untitled-ontology-33#Hobby";
        Integer num = 1;
        String individual = "http://www.semanticweb.org/mmar/ontologies/2020/3/untitled-ontology-33#Painting";

        assertEquals(cls, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("cls")));
        assertEquals(num, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("num")));
        assertEquals(individual, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("individual")));
    }

}