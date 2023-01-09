package loader.impl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import openllet.owlapi.OpenlletReasoner;
import openllet.owlapi.OpenlletReasonerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;


@ExtendWith(MockitoExtension.class)
public class LoaderObjectPropertyInversesTest {

    final File ontology = new File("src/test/resources/LoaderObjectPropertyInverses/ontology.owl");

    @Mock
    private Database mockedDatabase;

    @BeforeEach
    void setup() {
        Mockito.lenient().when(mockedDatabase.getDatabaseName()).thenReturn("test");
    }

    @Test
    void shouldReturnClassIndividuals() throws OWLOntologyCreationException {
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

        // Read ontology from classpath
        OWLOntology knowledgeGraph = manager.loadOntologyFromOntologyDocument(ontology);
        OpenlletReasoner reasoner = OpenlletReasonerFactory.getInstance().createReasoner(knowledgeGraph);

        List<SimpleStatement> results = new LoaderObjectPropertyInverses(mockedDatabase, manager, reasoner)
                .loadOWLOntology(knowledgeGraph)
                .collect(Collectors.toList());

        assertEquals(2, results.size());

        String p = "http://www.semanticweb.org/mmar/ontologies/2020/4/untitled-ontology-57#p2";
        String i = "http://www.semanticweb.org/mmar/ontologies/2020/4/untitled-ontology-57#p1";

        assertEquals(i, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("prop")));
        assertEquals(p, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("inverse")));

        assertEquals(p, results.get(1).getNamedValues().get(CqlIdentifier.fromCql("prop")));
        assertEquals(i, results.get(1).getNamedValues().get(CqlIdentifier.fromCql("inverse")));
    }

}