package com.bd.search;

public class SearchServiceTest {
    
    public static void main(String[] args) {
        System.out.println("Testing Search Service setup...");
        
        try {
            String port = DatabaseConfig.getProperty("server.port", "7002");
            System.out.println("✓ Configuration loaded. Port: " + port);
        } catch (Exception e) {
            System.err.println("✗ Configuration failed: " + e.getMessage());
        }
        
        try {
            DatabaseConfig.getConnection().close();
            System.out.println("✓ Database connection successful");
        } catch (Exception e) {
            System.err.println("✗ Database connection failed: " + e.getMessage());
        }
        
        try {
            new SearchService();
            System.out.println("✓ Search service created successfully");
        } catch (Exception e) {
            System.err.println("✗ Search service failed: " + e.getMessage());
        }
        
        System.out.println("Test completed.");
    }
}