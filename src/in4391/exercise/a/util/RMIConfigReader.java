package in4391.exercise.a.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Reads a config file in .properties format.
 */
public class RMIConfigReader
{
    private Properties config;
    private static final String filename = "system.properties";

    /**
     * Create a config reader for the given config file.
     *
     * @throws java.io.IOException
     */
    public RMIConfigReader() throws IOException
    {
        config = new Properties();
        config.load(new FileInputStream(filename));
    }

    public String get(String prop)
    {
        return config.getProperty(prop);
    }
}