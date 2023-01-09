package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import openllet.owlapi.OpenlletReasoner;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import table.impl.IsObjectPropertyDomain;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;


public class LoaderObjectPropertyDomains extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderObjectPropertyDomains.class);

    private static IsObjectPropertyDomain isObjectPropertyDomain;

    public LoaderObjectPropertyDomains(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        super(connection, manager, reasoner);
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        Set<SimpleStatement> collection = new HashSet<>();

        // `objectPropertiesInSignature` will return named properties
        for (OWLObjectProperty property : ontology.objectPropertiesInSignature().toArray(OWLObjectProperty[]::new)) {
            LOGGER.debug("Found property " + property + " in signature");

            // For each object property, return its domains
            // (note that it includes inferred domains, such as subPropertyOf)
            for (OWLClass domain : reasoner.objectPropertyDomains(property, true).toArray(OWLClass[]::new)) {
                HashMap<String, Object> assignments = new HashMap<>();
                assignments.put("property", property.getIRI().getIRIString());
                assignments.put("domain", domain.getIRI().getIRIString());

                LOGGER.debug("Property " + property + " has domain " + domain);

                SimpleStatement query = isObjectPropertyDomain.statementIncrementalInsert(assignments);
                collection.add(query);
            }
        }

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        isObjectPropertyDomain = new IsObjectPropertyDomain(connection);
        isObjectPropertyDomain.initialize();
    }

}
