package loader;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import database.Database;
import openllet.owlapi.OpenlletReasoner;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import java.util.stream.Stream;

public abstract class LoaderManager {

    public final Database connection;
    public final OpenlletReasoner reasoner;
    public final OWLOntologyManager manager;

    public LoaderManager(Database connection) {
        this.connection = connection;
        this.manager = null;
        this.reasoner = null;

        initializeTables();
    }

    public LoaderManager(Database connection, OWLOntologyManager manager) {
        this.connection = connection;
        this.manager = manager;
        this.reasoner = null;

        initializeTables();
    }

    public LoaderManager(Database connection, OWLOntologyManager manager, OpenlletReasoner reasoner) {
        this.connection = connection;
        this.manager = manager;
        this.reasoner = reasoner;

        initializeTables();
    }

    public abstract Stream<SimpleStatement> loadOWLOntology(OWLOntology ontology);

    public abstract void initializeTables();

}