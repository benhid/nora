package loader.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import loader.LoaderManager;
import openllet.owlapi.OpenlletReasoner;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.NodeSet;
import table.impl.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class LoaderSubClasses extends LoaderManager {

    private final static Logger LOGGER = Logger.getLogger(LoaderSubClasses.class);

    private static IsSubclassOfAll isSubclassOfAll;
    private static IsSubclassOfClass isSubclassOfClass;
    private static IsSubclassOfIntersection isSubclassOfIntersection;
    private static IsSubclassOfSome isSubclassOfSome;
    private static IsSubclassOfUnion isSubclassOfUnion;
    private static IsSubclassOfMaxCardinality isSubclassOfMaxCardinality;
    private static IsSubclassOfMinCardinality isSubclassOfMinCardinality;
    private static IsSubclassOfComplement isSubclassOfComplement;

    private final LoaderEquivalentClasses loaderEquivalentClasses;

    public LoaderSubClasses(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        super(connection, manager, reasoner);
        this.loaderEquivalentClasses = new LoaderEquivalentClasses(connection, manager, reasoner);
    }

    public Set<SimpleStatement> classifyClassExpression(String key, OWLClassExpression expression) {
        Set<SimpleStatement> collection = new HashSet<>();

        LOGGER.debug("Classifying class expression: " + expression);

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
                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, f));
            } else {
                assignments.put("range", f.asOWLClass().getIRI().getIRIString());
            }

            if (p.isAnonymous()) {
                String k = getRandomString();
                assignments.put("prop", k);
                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, f));
            } else {
                assignments.put("prop", p.asOWLObjectProperty().getIRI().getIRIString());
            }

            query = isSubclassOfAll.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectUnionOf) {
            LOGGER.debug(expression + " is instance of OWLObjectUnionOf");

            OWLObjectUnionOf objectUnionOf = (OWLObjectUnionOf) expression;
            List<OWLClassExpression> operands = objectUnionOf.operands().collect(Collectors.toList());

            assignments.put("cls", key);

            if (operands.get(0).isAnonymous()) {
                String k = getRandomString();
                assignments.put("ind1", k);
                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, operands.get(0)));
            } else {
                assignments.put("ind1", operands.get(0).asOWLClass().getIRI().getIRIString());
            }

            if (operands.size() > 2) {
                String k = getRandomString();
                assignments.put("ind2", k);

                List<OWLClassExpression> extraOperands = operands.subList(1, operands.size());
                OWLObjectUnionOf d = manager.getOWLDataFactory().getOWLObjectUnionOf(extraOperands);

                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, d));
            } else {
                if (operands.get(1).isAnonymous()) {
                    String k = getRandomString();
                    assignments.put("ind2", k);
                    collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, operands.get(1)));
                } else {
                    assignments.put("ind2", operands.get(1).asOWLClass().getIRI().getIRIString());
                }
            }

            query = isSubclassOfUnion.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectIntersectionOf) {
            LOGGER.debug(expression + " is instance of OWLObjectIntersectionOf");

            OWLObjectIntersectionOf objectIntersectionOf = (OWLObjectIntersectionOf) expression;
            List<OWLClassExpression> operands = objectIntersectionOf.operands().collect(Collectors.toList());

            assignments.put("cls", key);

            if (operands.get(0).isAnonymous()) {
                String k = getRandomString();
                assignments.put("ind1", k);
                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, operands.get(0)));
            } else {
                assignments.put("ind1", operands.get(0).asOWLClass().getIRI().getIRIString());
            }

            if (operands.size() > 2) {
                String k = getRandomString();
                assignments.put("ind2", k);

                List<OWLClassExpression> extraOperands = operands.subList(1, operands.size());
                OWLObjectIntersectionOf d = manager.getOWLDataFactory().getOWLObjectIntersectionOf(extraOperands);

                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, d));
            } else {
                if (operands.get(1).isAnonymous()) {
                    String k = getRandomString();
                    assignments.put("ind2", k);
                    collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, operands.get(1)));
                } else {
                    assignments.put("ind2", operands.get(1).asOWLClass().getIRI().getIRIString());
                }
            }

            query = isSubclassOfIntersection.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectComplementOf) {
            LOGGER.debug(expression + " is instance of OWLObjectComplementOf");

            OWLObjectComplementOf complementOf = (OWLObjectComplementOf) expression;
            OWLClassExpression f = complementOf.getOperand();

            assignments.put("key", key);

            if (f.isAnonymous()) {
                String k = getRandomString();
                assignments.put("complement", k);
                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, f));
            } else {
                assignments.put("complement", f.asOWLClass().getIRI().getIRIString());
            }

            query = isSubclassOfComplement.statementIncrementalInsert(assignments);
        } else if (expression instanceof OWLObjectSomeValuesFrom) {
            LOGGER.debug(expression + " is instance of OWLObjectSomeValuesFrom");

            OWLObjectSomeValuesFrom someValuesFrom = (OWLObjectSomeValuesFrom) expression;
            OWLObjectPropertyExpression p = someValuesFrom.getProperty();
            OWLClassExpression f = someValuesFrom.getFiller();

            assignments.put("cls", key);

            if (f.isAnonymous()) {
                String k = getRandomString();
                assignments.put("range", k);
                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, f));
            } else {
                assignments.put("range", f.asOWLClass().getIRI().getIRIString());
            }

            if (p.isAnonymous()) {
                String k = getRandomString();
                assignments.put("prop", k);
                collection.addAll(loaderEquivalentClasses.classifyClassExpression(k, (OWLClassExpression) p));
            } else {
                assignments.put("prop", p.asOWLObjectProperty().getIRI().getIRIString());
            }

            query = isSubclassOfSome.statementIncrementalInsert(assignments);
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

            query = isSubclassOfMaxCardinality.statementIncrementalInsert(assignments);
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

            query = isSubclassOfMinCardinality.statementIncrementalInsert(assignments);
        } else {
            throw new NotImplementedException("Axiom expression does not match any known restriction");
        }

        collection.add(query);

        return collection;
    }

    public Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology) {
        Set<SimpleStatement> collection = new HashSet<>();

        for (OWLClass currClass : ontology.classesInSignature().toArray(OWLClass[]::new)) {
            LOGGER.debug("Found class " + currClass + " in signature");

            // Step 1
            //  Store subclass axioms for subclasses on the ontology
            for (OWLSubClassOfAxiom axiom : ontology.subClassAxiomsForSubClass(currClass).toArray(OWLSubClassOfAxiom[]::new)) {
                OWLClassExpression superClass = axiom.getSuperClass();

                String key = currClass.getIRI().toString();

                LOGGER.debug("Got axiom " + axiom);

                if (superClass.isAnonymous()) {
                    LOGGER.debug(currClass + " is subclass of anonymous " + superClass);
                    collection.addAll(classifyClassExpression(key, superClass));
                } else {
                    LOGGER.debug(currClass + " is subclass of " + superClass);

                    HashMap<String, Object> assignments = new HashMap<>();

                    assignments.put("cls", key);
                    assignments.put("supclass", superClass.asOWLClass().getIRI().getIRIString());

                    SimpleStatement query = isSubclassOfClass.statementIncrementalInsert(assignments);
                    collection.add(query);
                }
            }

            // Step 2
            //  Store inferred axioms for subclasses
            NodeSet<OWLClass> subClasses = reasoner.getSubClasses(currClass, true);

            for (OWLClass subClass : subClasses.entities().toArray(OWLClass[]::new)) {
                if (subClass.isOWLNothing()) {
                    LOGGER.debug("Discarding subclass owl:Nothing");
                    continue;
                }

                // We only want to store *NEW* inferred axioms and discard the rest (we already store them in Step 1)
                OWLSubClassOfAxiom subClassOfAxiom = manager.getOWLDataFactory().getOWLSubClassOfAxiom(subClass, currClass);

                if (ontology.containsAxiom(subClassOfAxiom)) {
                    LOGGER.warn("Skipping inferred axiom " + subClassOfAxiom + " already present in the ontology");
                    continue;
                }

                LOGGER.debug(subClass + " is inferred subclass of " + currClass);

                String key = subClass.getIRI().toString();

                HashMap<String, Object> assignments = new HashMap<>();

                assignments.put("cls", key);
                assignments.put("supclass", currClass.asOWLClass().getIRI().getIRIString());

                SimpleStatement query = isSubclassOfClass.statementIncrementalInsert(assignments);
                collection.add(query);
            }
        }

        LOGGER.info("Found " + collection.size() + " statements to insert");

        return collection.stream();
    }

    public void initializeTables() {
        isSubclassOfClass = new IsSubclassOfClass(connection);
        isSubclassOfClass.initialize();

        isSubclassOfAll = new IsSubclassOfAll(connection);
        isSubclassOfAll.initialize();

        isSubclassOfUnion = new IsSubclassOfUnion(connection);
        isSubclassOfUnion.initialize();

        isSubclassOfIntersection = new IsSubclassOfIntersection(connection);
        isSubclassOfIntersection.initialize();

        isSubclassOfMaxCardinality = new IsSubclassOfMaxCardinality(connection);
        isSubclassOfMaxCardinality.initialize();

        isSubclassOfMinCardinality = new IsSubclassOfMinCardinality(connection);
        isSubclassOfMinCardinality.initialize();

        isSubclassOfComplement = new IsSubclassOfComplement(connection);
        isSubclassOfComplement.initialize();

        isSubclassOfSome = new IsSubclassOfSome(connection);
        isSubclassOfSome.initialize();
    }

    public String getRandomString() {
        return "_" + RandomStringUtils.random(5, true, false);
    }

}
