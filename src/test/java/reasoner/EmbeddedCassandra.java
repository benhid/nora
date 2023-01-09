package reasoner;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.JedisPool;

@ExtendWith(MockitoExtension.class)
@ExtendWith({EmbeddedCassandraExtension.class})
public class EmbeddedCassandra {

    protected String host = EmbeddedCassandraExtension.host;
    protected Integer port = EmbeddedCassandraExtension.port;
    protected String username = EmbeddedCassandraExtension.username;
    protected String password = EmbeddedCassandraExtension.password;

    @Mock
    protected JedisPool mockedJedis;

}