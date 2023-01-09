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
public class LoaderObjectPropertySubPropertiesTest {

    final File ontology = new File("src/test/resources/LoaderObjectPropertySubProperties/ontology.owl");

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

        List<SimpleStatement> results = new LoaderObjectPropertySubProperties(mockedDatabase, manager, reasoner)
                .loadOWLOntology(knowledgeGraph)
                .collect(Collectors.toList());

        assertEquals(1, results.size());

        String prop = "http://www.semanticweb.org/mmar/ontologies/2020/3/untitled-ontology-33#hasUndergraduateDegreeFrom";
        String subProp = "http://www.semanticweb.org/mmar/ontologies/2020/3/untitled-ontology-33#hasDegreeFrom";

        assertEquals(prop, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("prop")));
        assertEquals(subProp, results.get(0).getNamedValues().get(CqlIdentifier.fromCql("subprop")));
    }

}