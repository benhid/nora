package database.impl;

import database.DBInitException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class CassandraTest {

    private static Cassandra connection;

    @BeforeAll
    static void before() {
        connection = new Cassandra("hostname", 8000, "username", "password", "Test");
    }

    @Test
    void shouldThrowExceptionWhenConnectingToDatabase() {
        Throwable thrown = assertThrows(DBInitException.class, () -> connection.connect());
        assertTrue(thrown.getMessage().contains("Unable to connect to database"));
    }

    @Test
    void shouldReturnHost() {
        assertEquals(
                "hostname",
                connection.getHost());
    }

    @Test
    void shouldReturnPort() {
        assertEquals(
                8000,
                connection.getPort());
    }

    @Test
    void shouldReturnUsername() {
        assertEquals(
                "username",
                connection.getUsername());
    }

    @Test
    void shouldReturnPassword() {
        assertEquals(
                "password",
                connection.getPassword());
    }

    @Test
    void shouldReturnDatabaseNameLowerCase() {
        assertEquals(
                "test",
                connection.getDatabaseName());
    }

}
