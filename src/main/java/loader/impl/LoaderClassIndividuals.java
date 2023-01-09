package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.model.parameters.Imports;
import table.impl.ClassIndividuals;
import table.impl.IsSameAs;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;


public class LoaderClassIndividuals extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderClassIndividuals.class);

    private static ClassIndividuals classIndividual;
    private static IsSameAs isSameAs;

    public LoaderClassIndividuals(Database db, OWLOntologyManager manager) {
        super(db, manager);
    }

    /**
     * Loads `class` and `same as` assertion axioms. Imports are included.
     */
    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        return load(ontology, Imports.INCLUDED);
    }

    /**
     * Loads `class` and `same as` assertion axioms.
     */
    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology, Imports imports) {
        return load(ontology, imports);
    }

    private Stream<SimpleStatement> load(OWLOntology ontology, Imports imports) {
        Set<SimpleStatement> collection = new HashSet<>();

        // `axioms` will return class assertions from the ontology
        Stream<OWLClassAssertionAxiom> classAxioms = ontology.axioms(AxiomType.CLASS_ASSERTION, imports);

        classAxioms.forEach(axiom -> {
            LOGGER.debug("Found class assertion axiom " + axiom);

            OWLClassExpression cls = axiom.getClassExpression();

            for (OWLNamedIndividual individual : axiom.individualsInSignature().toArray(OWLNamedIndividual[]::new)) {
                HashMap<String, Object> assignments = new HashMap<>();
                if (!cls.isOWLThing()) {
                    LOGGER.debug("Found individual " + individual + " of class " + cls);

                    assignments.put("cls", cls.asOWLClass().getIRI().getIRIString());
                    assignments.put("individual", individual.getIRI().getIRIString());

                    SimpleStatement query = classIndividual.statementIncrementalInsert(assignments);
                    collection.add(query);
                } else {
                    LOGGER.debug("Discarding expression owl:Thing");
                }
            }
        });

        // `axioms` will return same as assertions from the ontology
        Stream<OWLSameIndividualAxiom> sameAsAxioms = ontology.axioms(AxiomType.SAME_INDIVIDUAL, imports);

        sameAsAxioms.forEach(axiom -> {
            LOGGER.debug("Found same as assertion axiom " + axiom);

            List<OWLIndividual> operands = axiom.getOperandsAsList();

            OWLIndividual individual1 = operands.get(0);
            OWLIndividual individual2 = operands.get(1);

            HashMap<String, Object> assignments = new HashMap<>();

            assignments.put("ind", individual1.asOWLNamedIndividual().getIRI().getIRIString());
            assignments.put("same", individual2.asOWLNamedIndividual().getIRI().getIRIString());

            SimpleStatement query = isSameAs.statementIncrementalInsert(assignments);
            collection.add(query);

            // swap positions

            assignments.put("ind", individual2.asOWLNamedIndividual().getIRI().getIRIString());
            assignments.put("same", individual1.asOWLNamedIndividual().getIRI().getIRIString());

            query = isSameAs.statementIncrementalInsert(assignments);
            collection.add(query);
        });

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        classIndividual = new ClassIndividuals(connection);
        classIndividual.initialize();

        isSameAs = new IsSameAs(connection);
        isSameAs.initialize();
    }

}
