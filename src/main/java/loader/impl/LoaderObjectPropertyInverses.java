package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import openllet.owlapi.OpenlletReasoner;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyExpression;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import table.impl.IsObjectPropertyInverseOf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;


public class LoaderObjectPropertyInverses extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderObjectPropertyInverses.class);

    private static IsObjectPropertyInverseOf isObjectPropertyInverseOf;

    public LoaderObjectPropertyInverses(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        super(connection, manager, reasoner);
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        Set<SimpleStatement> collection = new HashSet<>();

        // `objectPropertiesInSignature` will return named properties
        for (OWLObjectProperty property : ontology.objectPropertiesInSignature().toArray(OWLObjectProperty[]::new)) {
            LOGGER.debug("Found object property " + property + " in signature");

            for (OWLObjectPropertyExpression inverseProperty : reasoner.getInverseObjectProperties(property)) {
                HashMap<String, Object> assignments = new HashMap<>();

                String prop = property.getIRI().getIRIString();
                String inverse = inverseProperty.getNamedProperty().getIRI().getIRIString();

                if (!prop.equals(inverse)) {
                    LOGGER.debug("Property " + property + " is inverse of " + inverseProperty);

                    assignments.put("prop", prop);
                    assignments.put("inverse", inverse);

                    SimpleStatement query = isObjectPropertyInverseOf.statementIncrementalInsert(assignments);
                    collection.add(query);
                }
            }
        }

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        isObjectPropertyInverseOf = new IsObjectPropertyInverseOf(connection);
        isObjectPropertyInverseOf.initialize();
    }

}
