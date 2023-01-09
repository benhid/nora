package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import openllet.owlapi.OpenlletReasoner;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.*;
import table.impl.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class LoaderEquivalentClasses extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderEquivalentClasses.class);

    private static IsEquivalentToAll isEquivalentToAll;
    private static IsEquivalentToClass isEquivalentToClass;
    private static IsEquivalentToIntersection isEquivalentToIntersection;
    private static IsEquivalentToSome isEquivalentToSome;
    private static IsEquivalentToUnion isEquivalentToUnion;
    private static IsEquivalentToMaxCardinality isEquivalentToMaxCardinality;
    private static IsEquivalentToMinCardinality isEquivalentToMinCardinality;
    private static IsComplementOf isComplementOf;

    public LoaderEquivalentClasses(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        super(connection, manager, reasoner);
    }

    public Set<SimpleStatement> classifyClassExpression(String key, OWLClassExpression expression) {
        Set<SimpleStatement> collection = new HashSet<>();

        LOGGER.info("Classifying class expression: " + expression);

        HashMap<String, Object> assignments = new HashMap<>();
        SimpleStatement query;

        if (expression instanceof OWLObjectAllValuesFrom) {
            LOGGER.debug(expression + " is instance of OWLObjectAllValuesFrom");

            OWLObjectAllValuesFrom objectAllValuesFrom = (OWLObjectAllValuesFrom) expression;
            OWLObjectPropertyExpression p = objectAllValuesFrom.getProperty();
            OWLClassExpression f = objectAllValuesFrom.getFiller();

            assignments.put("cls", key);

            if (f.isAnonymous()) {
                String k = getRandomString();
                assignments.put("range", k);
                collection.addAll(classifyClassExpression(k, f));
            } else {
                assignments.put("range", f.asOWLClass().getIRI().getIRIString());
            }

            if (p.isAnonymous()) {
                String k = getRandomString();
                assignments.put("prop", k);
                collection.addAll(classifyClassExpression(k, (OWLClassExpression) p));
            } else {
                assignments.put("prop", p.asOWLObjectProperty().getIRI().getIRIString());
            }

            query = isEquivalentToAll.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectUnionOf) {
            LOGGER.debug(expression + " is instance of OWLObjectUnionOf");

            OWLObjectUnionOf objectUnionOf = (OWLObjectUnionOf) expression;
            List<OWLClassExpression> operands = objectUnionOf.operands().collect(Collectors.toList());

            assignments.put("cls", key);

            if (operands.get(0).isAnonymous()) {
                String k = getRandomString();
                assignments.put("ind1", k);
                collection.addAll(classifyClassExpression(k, operands.get(0)));
            } else {
                assignments.put("ind1", operands.get(0).asOWLClass().getIRI().getIRIString());
            }

            if (operands.size() > 2) {
                String k = getRandomString();
                assignments.put("ind2", k);

                List<OWLClassExpression> extraOperands = operands.subList(1, operands.size());
                OWLObjectUnionOf d = manager.getOWLDataFactory().getOWLObjectUnionOf(extraOperands);

                collection.addAll(classifyClassExpression(k, d));
            } else {
                if (operands.get(1).isAnonymous()) {
                    String k = getRandomString();
                    assignments.put("ind2", k);
                    collection.addAll(classifyClassExpression(k, operands.get(1)));
                } else {
                    assignments.put("ind2", operands.get(1).asOWLClass().getIRI().getIRIString());
                }
            }

            query = isEquivalentToUnion.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectIntersectionOf) {
            LOGGER.debug(expression + " is instance of OWLObjectIntersectionOf");

            OWLObjectIntersectionOf objectIntersectionOf = (OWLObjectIntersectionOf) expression;
            List<OWLClassExpression> operands = objectIntersectionOf.operands().collect(Collectors.toList());

            assignments.put("cls", key);

            if (operands.get(0).isAnonymous()) {
                String k = getRandomString();
                assignments.put("ind1", k);
                collection.addAll(classifyClassExpression(k, operands.get(0)));
            } else {
                assignments.put("ind1", operands.get(0).asOWLClass().getIRI().getIRIString());
            }

            if (operands.size() > 2) {
                String k = getRandomString();
                assignments.put("ind2", k);

                List<OWLClassExpression> extraOperands = operands.subList(1, operands.size());
                OWLObjectIntersectionOf d = manager.getOWLDataFactory().getOWLObjectIntersectionOf(extraOperands);

                collection.addAll(classifyClassExpression(k, d));
            } else if (operands.size() == 2) {
                // There is only one operand left, which is ind2
                if (operands.get(1).isAnonymous()) {
                    String k = getRandomString();
                    assignments.put("ind2", k);
                    collection.addAll(classifyClassExpression(k, operands.get(1)));
                } else {
                    assignments.put("ind2", operands.get(1).asOWLClass().getIRI().getIRIString());
                }
            } else {
                // There are no operands left, not sure about what to do here
                assignments.put("ind2", "");
                LOGGER.warn("OWLObjectIntersectionOf has no operands: " + expression);
            }

            query = isEquivalentToIntersection.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectComplementOf) {
            LOGGER.debug(expression + " is instance of OWLObjectComplementOf");

            OWLObjectComplementOf complementOf = (OWLObjectComplementOf) expression;
            OWLClassExpression f = complementOf.getOperand();

            assignments.put("key", key);

            if (f.isAnonymous()) {
                String k = getRandomString();
                assignments.put("complement", k);
                collection.addAll(classifyClassExpression(k, f));
            } else {
                assignments.put("complement", f.asOWLClass().getIRI().getIRIString());
            }

            query = isComplementOf.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectSomeValuesFrom) {
            LOGGER.debug(expression + " is instance of OWLObjectSomeValuesFrom");

            OWLObjectSomeValuesFrom someValuesFrom = (OWLObjectSomeValuesFrom) expression;
            OWLObjectPropertyExpression p = someValuesFrom.getProperty();
            OWLClassExpression f = someValuesFrom.getFiller();

            assignments.put("cls", key);

            if (f.isAnonymous()) {
                String k = getRandomString();
                assignments.put("range", k);
                collection.addAll(classifyClassExpression(k, f));
            } else {
                assignments.put("range", f.asOWLClass().getIRI().getIRIString());
            }

            if (p.isAnonymous()) {
                String k = getRandomString();
                assignments.put("prop", k);
                collection.addAll(classifyClassExpression(k, (OWLClassExpression) p));
            } else {
                assignments.put("prop", p.asOWLObjectProperty().getIRI().getIRIString());
            }

            query = isEquivalentToSome.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectMinCardinality) {
            LOGGER.debug(expression + " is instance of OWLObjectMinCardinality");

            OWLObjectMinCardinality minCardinality = (OWLObjectMinCardinality) expression;
            OWLObjectPropertyExpression p = minCardinality.getProperty();
            OWLClassExpression f = minCardinality.getFiller();
            int cardinality = minCardinality.getCardinality();

            assignments.put("cls", key);
            assignments.put("card", String.valueOf(cardinality));

            if (f.isAnonymous()) {
                String k = getRandomString();
                assignments.put("clss", k);
                collection.addAll(classifyClassExpression(k, f));
            } else {
                assignments.put("clss", f.asOWLClass().getIRI().getIRIString());
            }

            if (p.isAnonymous()) {
                String k = getRandomString();
                assignments.put("prop", k);
                collection.addAll(classifyClassExpression(k, (OWLClassExpression) p));
            } else {
                assignments.put("prop", p.asOWLObjectProperty().getIRI().getIRIString());
            }

            query = isEquivalentToMinCardinality.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectMaxCardinality) {
            LOGGER.debug(expression + " is instance of OWLObjectMaxCardinality");

            OWLObjectMaxCardinality maxCardinality = (OWLObjectMaxCardinality) expression;
            OWLObjectPropertyExpression p = maxCardinality.getProperty();
            OWLClassExpression f = maxCardinality.getFiller();
            int cardinality = maxCardinality.getCardinality();

            assignments.put("cls", key);
            assignments.put("card", String.valueOf(cardinality));

            if (f.isAnonymous()) {
                String k = getRandomString();
                assignments.put("clss", k);
                collection.addAll(classifyClassExpression(k, f));
            } else {
                assignments.put("clss", f.asOWLClass().getIRI().getIRIString());
            }

            if (p.isAnonymous()) {
                String k = getRandomString();
                assignments.put("prop", k);
                collection.addAll(classifyClassExpression(k, (OWLClassExpression) p));
            } else {
                assignments.put("prop", p.asOWLObjectProperty().getIRI().getIRIString());
            }

            query = isEquivalentToMaxCardinality.statementIncrementalInsert(assignments);
        } else {
            throw new NotImplementedException("Expression does not match any known restriction");
        }

        collection.add(query);

        return collection;
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        Set<SimpleStatement> collection = new HashSet<>();

        // equivalent class axioms
        for (OWLClass currClass : ontology.classesInSignature().toArray(OWLClass[]::new)) {
            // Get equivalent classes axioms that contains the class as an operand (in any position)
            for (OWLEquivalentClassesAxiom axiom : ontology.equivalentClassesAxioms(currClass).toArray(OWLEquivalentClassesAxiom[]::new)) {
                LOGGER.debug("Found equivalent class assertion axiom " + axiom);

                List<OWLClassExpression> operands = axiom.getOperandsAsList();

                OWLClassExpression property = operands.get(0);  // same as `currClass`
                OWLClassExpression filler = operands.get(1);

                String key = property.asOWLClass().getIRI().getIRIString();

                if (filler.isAnonymous()) {
                    LOGGER.debug(filler + " is anonymous");
                    collection.addAll(classifyClassExpression(key, filler));
                } else {
                    LOGGER.debug(filler + " is OWLClass");

                    // This one is "tricky":
                    //  When we got all equivalentClassesAxioms for our class, the OWLAPI returned
                    //   all axioms in which our class takes part of as *EITHER* first OR second operand.
                    //  Thus, if have A=B, an run equivalentClassesAxioms(A) we'll get:
                    //   * EquivalentClasses(A, B)
                    //  and if we do equivalentClassesAxioms(B) we'll get:
                    //   * EquivalentClasses(A, B)
                    //  (i.e., order is not preserved).
                    //  This means that we have to check if our class is first or second operand before saving
                    //   the results to the database.

                    OWLClass owlClass = filler.asOWLClass();
                    String equiv = owlClass.getIRI().toString();

                    HashMap<String, Object> assignments = new HashMap<>();

                    if (currClass.toString().equals(key)) {
                        // Class is the first operand in the axiom
                        assignments.put("cls", key);
                        assignments.put("equiv", equiv);
                    } else {
                        // Class is the second operand in the axiom (invert positions in query)
                        assignments.put("cls", equiv);
                        assignments.put("equiv", key);
                    }

                    assignments.put("num", 1); // todo This is really necessary?

                    SimpleStatement query = isEquivalentToClass.statementInsert(assignments);

                    collection.add(query);
                }
            }
        }

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        isEquivalentToAll = new IsEquivalentToAll(connection);
        isEquivalentToAll.initialize();

        isEquivalentToClass = new IsEquivalentToClass(connection);
        isEquivalentToClass.initialize();

        isEquivalentToIntersection = new IsEquivalentToIntersection(connection);
        isEquivalentToIntersection.initialize();

        isEquivalentToSome = new IsEquivalentToSome(connection);
        isEquivalentToSome.initialize();

        isEquivalentToUnion = new IsEquivalentToUnion(connection);
        isEquivalentToUnion.initialize();

        isEquivalentToMaxCardinality = new IsEquivalentToMaxCardinality(connection);
        isEquivalentToMaxCardinality.initialize();

        isEquivalentToMinCardinality = new IsEquivalentToMinCardinality(connection);
        isEquivalentToMinCardinality.initialize();

        isComplementOf = new IsComplementOf(connection);
        isComplementOf.initialize();
    }

    public String getRandomString() {
        return "_" + RandomStringUtils.random(5, true, false);
    }

}
