package database;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class Configuration {

    private final static Logger LOGGER = Logger.getLogger(Configuration.class);

    private static final String PROPERTY_FILE = "config.properties";
    private static Properties prop = null;

    private void loadProperties() {
        InputStream input;
        String propertyFile = System.getenv("NORA_CONFIG");
        if (propertyFile == null) {
            input = getClass().getClassLoader().getResourceAsStream(PROPERTY_FILE);
        } else {
            try {
                input = Files.newInputStream(Paths.get(propertyFile));
            } catch (IOException e) {
                LOGGER.error("Could not load property file " + propertyFile);
                return;
            }
        }
        prop = new Properties();
        try {
            prop.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getProperty(String propertyName) {
        if (prop == null) {
            loadProperties();
        }
        return prop.getProperty(propertyName);
    }

    public String getProperty(String propertyName, String defaultValue) {
        if (prop == null) {
            loadProperties();
        }
        return prop.getProperty(propertyName, defaultValue);
    }
}