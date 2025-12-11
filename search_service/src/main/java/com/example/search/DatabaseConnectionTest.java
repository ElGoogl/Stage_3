package com.example.search;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Database Connection Tester
 * Run this to verify your database configuration is correct
 */
public class DatabaseConnectionTest {
    
    public static void main(String[] args) {
        System.out.println("=== Database Connection Test ===\n");
        
        // Test configuration loading
        System.out.println("1. Testing configuration...");
        String dbUrl = DatabaseConfig.getProperty("db.url");
        String dbUsername = DatabaseConfig.getProperty("db.username");
        String tableName = DatabaseConfig.getProperty("db.table.books", "books");
        
        System.out.println("   Database URL: " + dbUrl);
        System.out.println("   Username: " + dbUsername);
        System.out.println("   Table: " + tableName);
        
        // Test database connection
        System.out.println("\n2. Testing database connection...");
        try (Connection conn = DatabaseConfig.getConnection()) {
            System.out.println("   ✅ Database connection successful!");
            
            // Test table existence and structure
            System.out.println("\n3. Testing table structure...");
            String sql = "SELECT COUNT(*) as total FROM " + tableName;
            try (PreparedStatement stmt = conn.prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {
                
                if (rs.next()) {
                    int count = rs.getInt("total");
                    System.out.println("   ✅ Table '" + tableName + "' found with " + count + " records");
                }
            }
            
            // Test table columns
            System.out.println("\n4. Testing required columns...");
            sql = "SELECT book_id, title, author, language, year FROM " + tableName + " LIMIT 1";
            try (PreparedStatement stmt = conn.prepareStatement(sql);
                 ResultSet rs = stmt.executeQuery()) {
                
                if (rs.next()) {
                    System.out.println("   ✅ All required columns found:");
                    System.out.println("      - book_id: " + rs.getInt("book_id"));
                    System.out.println("      - title: " + rs.getString("title"));
                    System.out.println("      - author: " + rs.getString("author"));
                    System.out.println("      - language: " + rs.getString("language"));
                    System.out.println("      - year: " + rs.getInt("year"));
                } else {
                    System.out.println("   ⚠️  Table is empty but structure is correct");
                }
            }
            
            System.out.println("\n✅ Database configuration is READY!");
            
        } catch (SQLException e) {
            System.err.println("   ❌ Database connection failed:");
            System.err.println("   Error: " + e.getMessage());
            System.err.println("\n💡 Please check:");
            System.err.println("   - MySQL server is running");
            System.err.println("   - Database name, username, password in application.properties");
            System.err.println("   - Table name '" + tableName + "' exists");
            System.err.println("   - Required columns: book_id, title, author, language, year");
        }
    }
}