package com.bd.search;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConfig {
    private static final Properties properties = new Properties();
    
    static {
        try (InputStream input = DatabaseConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            System.err.println("Failed to load configuration: " + e.getMessage());
        }
    }
    
    public static Connection getConnection() throws SQLException {
        String url = properties.getProperty("db.url", "jdbc:mysql://localhost:3306/metadata_db");
        String username = properties.getProperty("db.username", "bd");
        String password = properties.getProperty("db.password", "bd");
        
        return DriverManager.getConnection(url, username, password);
    }
    
    public static String getProperty(String key) {
        return properties.getProperty(key);
    }
    
    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}