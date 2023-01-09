package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import openllet.owlapi.OpenlletReasoner;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.*;
import table.impl.IsObjectPropertyEquivalentToClass;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;


public class LoaderObjectPropertyEquivalentClasses extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderObjectPropertyEquivalentClasses.class);

    private static IsObjectPropertyEquivalentToClass isObjectPropertyEquivalentToClass;

    public LoaderObjectPropertyEquivalentClasses(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        super(connection, manager, reasoner);
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        Set<SimpleStatement> collection = new HashSet<>();

        // equivalent object property axioms
        Stream<OWLEquivalentObjectPropertiesAxiom> axioms = ontology.axioms(AxiomType.EQUIVALENT_OBJECT_PROPERTIES);

        axioms.forEach(axiom -> {
            LOGGER.debug("Found equivalent object property assertion axiom " + axiom);

            List<OWLObjectPropertyExpression> operands = axiom.getOperandsAsList();

            OWLObjectPropertyExpression cls = operands.get(0);
            OWLObjectPropertyExpression filler = operands.get(1);

            String key = cls.asOWLObjectProperty().getIRI().getIRIString();

            OWLObjectProperty owlClass = filler.asOWLObjectProperty();
            String p = owlClass.getIRI().toString();

            HashMap<String, Object> assignments = new HashMap<>();
            assignments.put("cls", key);
            assignments.put("num", 1);
            assignments.put("equiv", p);

            SimpleStatement query = isObjectPropertyEquivalentToClass.statementInsert(assignments);

            collection.add(query);
        });

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        isObjectPropertyEquivalentToClass = new IsObjectPropertyEquivalentToClass(connection);
        isObjectPropertyEquivalentToClass.initialize();
    }

}
