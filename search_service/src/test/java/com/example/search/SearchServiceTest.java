package com.example.search;

public class SearchServiceTest {
    
    public static void main(String[] args) {
        System.out.println("Testing Search Service setup...");
        
        // Test configuration loading
        try {
            String port = DatabaseConfig.getProperty("server.port", "7002");
            System.out.println("✓ Configuration loaded successfully. Port: " + port);
        } catch (Exception e) {
            System.err.println("✗ Failed to load configuration: " + e.getMessage());
        }
        
        // Test database connection
        try {
            DatabaseConfig.getConnection().close();
            System.out.println("✓ Database connection successful");
        } catch (Exception e) {
            System.err.println("✗ Database connection failed: " + e.getMessage());
            System.err.println("  Make sure MySQL is running and credentials are correct");
        }
        
        // Test search service instantiation
        try {
            SearchService service = new SearchService();
            System.out.println("✓ Search service created successfully");
        } catch (Exception e) {
            System.err.println("✗ Failed to create search service: " + e.getMessage());
        }
        
        System.out.println("\nTest completed. Run the main App to start the service.");
    }
}