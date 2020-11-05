package com.dreamgames.royalmatchserverjobs.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ServerProperties {
    private static Properties properties;

    public static void init(String propertiesPath) throws IOException {
        properties = new Properties();
        var inputStream = new FileInputStream(propertiesPath);
        properties.load(inputStream);
    }

    public static String getString(String key) {
        return properties.getProperty(key);
    }

    public static boolean getBoolean(String key) {
        return Boolean.parseBoolean(properties.getProperty(key));
    }

    public static int getInt(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    public static long getLong(String key) {
        return Long.parseLong(properties.getProperty(key));
    }
}
