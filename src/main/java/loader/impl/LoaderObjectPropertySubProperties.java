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
import org.semanticweb.owlapi.reasoner.Node;
import table.impl.IsSubPropertyOf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;


public class LoaderObjectPropertySubProperties extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderObjectPropertySubProperties.class);

    private static IsSubPropertyOf isSubPropertyOf;

    public LoaderObjectPropertySubProperties(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        super(connection, manager, reasoner);
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        Set<SimpleStatement> collection = new HashSet<>();

        // `objectPropertiesInSignature` will return named properties
        for (OWLObjectProperty property : ontology.objectPropertiesInSignature().toArray(OWLObjectProperty[]::new)) {
            LOGGER.debug("Found object property " + property + " in signature");

            for (Node<OWLObjectPropertyExpression> subProperty : reasoner.getSuperObjectProperties(property)) {
                HashMap<String, Object> assignments = new HashMap<>();

                for (OWLObjectPropertyExpression subPropertyExpression : subProperty.entities().toArray(OWLObjectPropertyExpression[]::new)) {
                    if (!subProperty.isTopNode()) {
                        LOGGER.debug("Property " + property + " is subproperty of " + subPropertyExpression);

                        assignments.put("prop", property.getIRI().getIRIString());
                        assignments.put("subprop", subPropertyExpression.asOWLObjectProperty().getIRI().getIRIString());

                        SimpleStatement query = isSubPropertyOf.statementIncrementalInsert(assignments);
                        collection.add(query);
                    } else {
                        LOGGER.debug("Discarding expression owl:topObjectProperty");
                    }
                }
            }
        }

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        isSubPropertyOf = new IsSubPropertyOf(connection);
        isSubPropertyOf.initialize();
    }

}
