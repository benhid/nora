package loader;

import org.apache.log4j.Logger;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.model.parameters.Imports;
import org.semanticweb.owlapi.util.SimpleIRIMapper;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatsDBpedia {

    private final static Logger LOGGER = Logger.getLogger(StatsDBpedia.class);

    public static void main(String[] argv) {
        File ontology = new File("examples/DBpedia/dbpedia_2016-10.owl");

        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();

        // Manually map imports.
        IRI documentIRI = IRI.create(ontology);
        IRI ontologyIRI = IRI.create("http://dbpedia.org/ontology/");

        SimpleIRIMapper mapper = new SimpleIRIMapper(ontologyIRI, documentIRI);
        manager.getIRIMappers().add(mapper);

        LOGGER.info("Loading ontology");

        try {
            OWLOntology TBox = manager.loadOntologyFromOntologyDocument(ontology);
            LOGGER.info("Loaded from document");

            Stream<OWLAxiom> axioms = TBox.axioms(Imports.INCLUDED);

            List<AxiomType<?>> list = axioms.map(axiom -> axiom.getAxiomType()).collect(Collectors.toList());

            Set<AxiomType<?>> distinct = new HashSet<>(list);
            for (AxiomType<?> s : distinct) {
                System.out.println(s + ": " + Collections.frequency(list, s));
            }
        } catch (OWLOntologyCreationException e) {
            e.printStackTrace();
        }

        LOGGER.info("Loading mapping objects");

        File mappingBasedObjects = new File("examples/DBpedia/mappingbased_objects_wkd_uris_en.ttl");

        try {
            OWLOntology knowledgeGraph = manager.loadOntologyFromOntologyDocument(mappingBasedObjects);
            LOGGER.info("Loaded from document");

            Stream<OWLAxiom> axioms = knowledgeGraph.axioms(Imports.INCLUDED);

            List<AxiomType<?>> list = axioms.map(axiom -> axiom.getAxiomType()).collect(Collectors.toList());

            Set<AxiomType<?>> distinct = new HashSet<>(list);
            for (AxiomType<?> s : distinct) {
                System.out.println(s + ": " + Collections.frequency(list, s));
            }
        } catch (OWLOntologyCreationException e) {
            e.printStackTrace();
        }

        LOGGER.info("Loading mapping literals");

        File mappingBasedLiterals = new File("examples/DBpedia/mappingbased-literals_lang=en_2016-10.ttl");

        try {
            OWLOntology knowledgeGraph = manager.loadOntologyFromOntologyDocument(mappingBasedLiterals);

            LOGGER.info("Loaded from document");

            Stream<OWLAxiom> axioms = knowledgeGraph.axioms(Imports.INCLUDED);

            // axioms.forEach(axiom -> {
            // LOGGER.info("Found axiom " + axiom);
            // });

            List<AxiomType<?>> list = axioms.map(axiom -> axiom.getAxiomType()).collect(Collectors.toList());

            Set<AxiomType<?>> distinct = new HashSet<>(list);
            for (AxiomType<?> s : distinct) {
                System.out.println(s + ": " + Collections.frequency(list, s));
            }
        } catch (OWLOntologyCreationException e) {
            e.printStackTrace();
        }
    }

}