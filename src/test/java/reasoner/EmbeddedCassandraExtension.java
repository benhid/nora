package reasoner;

import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Settings;
import com.github.nosan.embedded.cassandra.SimpleSeedProviderConfigurator;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@ExtendWith(MockitoExtension.class)
public class EmbeddedCassandraExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private final static Logger LOGGER = Logger.getLogger(EmbeddedCassandraExtension.class);

    // Gate keeper to prevent multiple Threads within the same routine
    final static Lock lock = new ReentrantLock();
    public static String host;
    public static Integer port;
    public static String username = "cassandra";
    public static String password = "cassandra";
    private static boolean started = false;
    private Cassandra embeddedCassandra;

    @Override
    public void beforeAll(final ExtensionContext context) {
        // Lock the access so only one Thread has access to it
        lock.lock();
        if (!started) {
            started = true;
            // The following line registers a callback hook when the root test context is
            // shut down
            String uniqueKey = this.getClass().getName();
            context.getRoot().getStore(GLOBAL).put(uniqueKey, this);
            // Startup process
            setup();
        }
        // Free the access
        lock.unlock();
    }

    /**
     * Callback that is invoked <em>exactly once</em>
     * before the start of <em>all</em> test containers.
     */
    private void setup() {
        LOGGER.info("Setting up Embedded Casandra instance");

        embeddedCassandra = new CassandraBuilder()
                .version("4.0.3")
                .addSystemProperty("cassandra.native_transport_port", 0)
                .addSystemProperty("cassandra.rpc_port", 0)
                .addSystemProperty("cassandra.storage_port", 0)
                .addSystemProperty("cassandra.jmx.local.port", 0)
                // Cassandra 4.0.3 supports JDK 11 or lower
                //.addEnvironmentVariable("JAVA_HOME", System.getenv("JDK_11"))
                .configure(new SimpleSeedProviderConfigurator("localhost:0"))
                .build();
        embeddedCassandra.start();

        Settings settings = embeddedCassandra.getSettings();
        host = "localhost";
        port = settings.getPort();
    }

    /**
     * Callback that is invoked <em>exactly once</em>
     * after the end of <em>all</em> test containers.
     * Inherited from {@code CloseableResource}
     */
    @Override
    public void close() {
        LOGGER.info("Stopping Embedded Casandra instance");
        if (embeddedCassandra.isRunning())
            embeddedCassandra.stop();
    }
}