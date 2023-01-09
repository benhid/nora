package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import openllet.owlapi.OpenlletReasoner;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.*;
import table.impl.IsDisjointWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;


public class LoaderDisjointClasses extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderDisjointClasses.class);

    private static IsDisjointWith isDisjointWith;

    public LoaderDisjointClasses(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        super(connection, manager, reasoner);
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        Set<SimpleStatement> collection = new HashSet<>();

        Stream<OWLDisjointClassesAxiom> axioms = ontology.axioms(AxiomType.DISJOINT_CLASSES);

        axioms.forEach(axiom -> {
            LOGGER.debug("Found class assertion axiom " + axiom);

            List<OWLClassExpression> operands = axiom.getOperandsAsList();

            OWLClassExpression cls = operands.get(0);
            OWLClassExpression individual = operands.get(1);

            HashMap<String, Object> assignments = new HashMap<>();
            assignments.put("cls", cls.asOWLClass().getIRI().getIRIString());
            assignments.put("ind1", individual.asOWLClass().getIRI().getIRIString());

            LOGGER.debug(individual + " is disjoint with " + cls);

            SimpleStatement query = isDisjointWith.statementIncrementalInsert(assignments);
            collection.add(query);
        });

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        isDisjointWith = new IsDisjointWith(connection);
        isDisjointWith.initialize();
    }

}
