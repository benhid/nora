package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import openllet.owlapi.OpenlletReasoner;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.NodeSet;
import table.impl.IsSuperClassOf;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;


public class LoaderSupClasses extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderSupClasses.class);

    private static IsSuperClassOf isSuperClassOf;

    public LoaderSupClasses(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        super(connection, manager, reasoner);
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        Set<SimpleStatement> collection = new HashSet<>();

        for (OWLClass currClass : ontology.classesInSignature().toArray(OWLClass[]::new)) {
            LOGGER.debug("Found class " + currClass + " in signature");
            NodeSet<OWLClass> supClasses = reasoner.getSuperClasses(currClass, true);

            for (OWLClass supClass : supClasses.entities().toArray(OWLClass[]::new)) {
                if (supClass.isOWLThing()) {
                    LOGGER.warn("Discarding individual with superclass owl:Thing");
                    continue;
                }

                LOGGER.debug(currClass + " is superclass of " + supClass);

                HashMap<String, Object> assignments = new HashMap<>();

                assignments.put("cls", supClass.getIRI().getIRIString());
                assignments.put("subclass", currClass.getIRI().getIRIString());

                SimpleStatement query = isSuperClassOf.statementIncrementalInsert(assignments);
                collection.add(query);
            }
        }

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        isSuperClassOf = new IsSuperClassOf(connection);
        isSuperClassOf.initialize();
    }

}
