package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.model.parameters.Imports;
import org.semanticweb.owlapi.search.EntitySearcher;
import table.impl.IsFunctionalProperty;
import table.impl.IsInverseFunctionalProperty;
import table.impl.PropIndividuals;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;


public class LoaderPropIndividuals extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderPropIndividuals.class);

    private static PropIndividuals propIndividuals;
    private static IsFunctionalProperty isFunctionalProperty;
    private static IsInverseFunctionalProperty isInverseFunctionalProperty;

    public LoaderPropIndividuals(Database db, OWLOntologyManager manager) {
        super(db, manager);
    }

    public static boolean isFunctional(OWLObjectPropertyExpression e, OWLOntology o) {
        return EntitySearcher.isFunctional(e, o.imports());
    }

    public static boolean isInverseFunctional(OWLObjectPropertyExpression e, OWLOntology o) {
        return EntitySearcher.isInverseFunctional(e, o.imports());
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology kb) {
        return load(kb, Imports.INCLUDED);
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology kb, Imports imports) {
        return load(kb, imports);
    }

    private Stream<SimpleStatement> load(OWLOntology kb, Imports imports) {
        Set<SimpleStatement> collection = new HashSet<>();

        // `axioms` will return object property assertions from the ontology
        Stream<OWLObjectPropertyAssertionAxiom> objectPropertyAxioms = kb.axioms(AxiomType.OBJECT_PROPERTY_ASSERTION, imports);

        objectPropertyAxioms.forEach(axiom -> {
            LOGGER.debug("Found object property axiom " + axiom);

            OWLIndividual object = axiom.getObject();
            OWLIndividual subject = axiom.getSubject();
            OWLObjectPropertyExpression property = axiom.getProperty();

            HashMap<String, Object> assignments = new HashMap<>();
            assignments.put("prop", property.asOWLObjectProperty().getIRI().getIRIString());

            if (isFunctional(property, kb)) {
                LOGGER.debug("Property " + property + " is declared as functional");
                SimpleStatement query = isFunctionalProperty.statementInsert(assignments);
                collection.add(query);
            } else if (isInverseFunctional(property, kb)) {
                LOGGER.debug("Property " + property + " is declared as inverse functional");
                SimpleStatement query = isInverseFunctionalProperty.statementInsert(assignments);
                collection.add(query);
            }

            assignments.put("range", object.asOWLNamedIndividual().getIRI().getIRIString());
            assignments.put("domain", subject.asOWLNamedIndividual().getIRI().getIRIString());

            SimpleStatement query = propIndividuals.statementIncrementalInsert(assignments);

            collection.add(query);
        });

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        propIndividuals = new PropIndividuals(connection);
        propIndividuals.initialize();

        isFunctionalProperty = new IsFunctionalProperty(connection);
        isFunctionalProperty.initialize();

        isInverseFunctionalProperty = new IsInverseFunctionalProperty(connection);
        isInverseFunctionalProperty.initialize();
    }

}
