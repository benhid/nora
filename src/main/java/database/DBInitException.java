package database;

public class DBInitException extends Exception {

    public DBInitException(String msg) {
        super("Unable to connect to database: " + msg);
    }

    public DBInitException() {
    }

}
