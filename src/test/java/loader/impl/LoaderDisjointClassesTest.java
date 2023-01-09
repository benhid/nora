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
public class LoaderDisjointClassesTest {

    final File ontology = new File("src/test/resources/LoaderDisjointClasses/ontology.owl");

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

        List<SimpleStatement> results = new LoaderDisjointClasses(mockedDatabase, manager, reasoner)
                .loadOWLOntology(knowledgeGraph)
                .collect(Collectors.toList());

        assertEquals(1, results.size());

        String cls = "http://www.semanticweb.org/mmar/ontologies/2020/6/disjoint#A";
        String ind1 = "http://www.semanticweb.org/mmar/ontologies/2020/6/disjoint#B";

        assertEquals(cls, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("cls")));
        assertEquals(ind1, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("ind1")));
    }

}