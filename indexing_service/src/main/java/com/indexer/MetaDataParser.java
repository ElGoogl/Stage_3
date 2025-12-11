package com.indexer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MetaDataParser{

    // Language code mapping for ISO 639-1 compliance
    private static final Map<String, String> LANGUAGE_CODES = Map.of(
        "English", "en",
        "French", "fr", 
        "German", "de",
        "Spanish", "es",
        "Italian", "it",
        "Portuguese", "pt",
        "Dutch", "nl",
        "Russian", "ru",
        "Chinese", "zh",
        "Japanese", "ja"
    );
    
    private static String convertToLanguageCode(String language) {
        if (language == null) return null;
        return LANGUAGE_CODES.getOrDefault(language.trim(), language.toLowerCase().trim());
    }

    private class DBUtil {
        private static final String URL = "jdbc:mysql://localhost:3306/metadata_db";
        private static final String USER = "bd";
        private static final String PASSWORD = "bd";

        public static Connection getConnection() throws SQLException {
            String url = System.getenv("DB_URL");
            String user = System.getenv("DB_USER");
            String password = System.getenv("DB_PASSWORD");
            return DriverManager.getConnection(URL, USER, PASSWORD);
        }
    }

    public static boolean storeMetadata(Map<String, String> metadata, long bookId) {
        String createTableSql = """
            CREATE TABLE IF NOT EXISTS book_metadata (
                book_id BIGINT PRIMARY KEY,
                title VARCHAR(512),
                author VARCHAR(256),
                language VARCHAR(8),
                year INT
            )
        """;
        String sql = """
            INSERT INTO book_metadata (
                book_id,
                title,
                author,
                language,
                year
            )
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                author = VALUES(author),
                language = VALUES(language),
                year = VALUES(year)
        """;

        try (Connection conn = DBUtil.getConnection()) {
            // Ensure table exists
            try (java.sql.Statement tableStmt = conn.createStatement()) {
                tableStmt.execute(createTableSql);
            }
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, bookId);
                stmt.setString(2, metadata.getOrDefault("title", null));
                stmt.setString(3, metadata.getOrDefault("author", null));
                stmt.setString(4, metadata.getOrDefault("language", null));

                if (metadata.containsKey("year")) {
                    stmt.setInt(5, Integer.parseInt(metadata.get("year")));
                } else {
                    stmt.setNull(5, java.sql.Types.INTEGER);
                }

                stmt.executeUpdate();
            }
            return true;
        } catch (Exception e) {
            System.err.println("Error storing metadata: " + e.getMessage());
            return false;
        }
    }

    /**
     * Only parses title, author, language, and publication year
     * Other data wasn't useful or was only found in 1% of books 
     */
    public static Map<String, String> parseMetadata(String text) {
        Map<String, String> metadata = new HashMap<>();

    Pattern titlePattern = Pattern.compile("^Title:\\s*(.+)$", Pattern.MULTILINE);
    Pattern authorPattern = Pattern.compile("^Author:\\s*(.+)$", Pattern.MULTILINE);
    Pattern languagePattern = Pattern.compile("^Language:\\s*(.+)$", Pattern.MULTILINE);
    Pattern releaseDatePattern = Pattern.compile("^Release date:\\s*(.+)$", Pattern.MULTILINE);

        Matcher m;

        m = titlePattern.matcher(text);
        if (m.find()) metadata.put("title", m.group(1).trim());

        m = authorPattern.matcher(text);
        if (m.find()) metadata.put("author", m.group(1).trim());

        m = languagePattern.matcher(text);
        if (m.find()) {
            String language = m.group(1).trim();
            String languageCode = convertToLanguageCode(language);
            metadata.put("language", languageCode);
        }

        // Extract year from 'Release date' field only
        m = releaseDatePattern.matcher(text);
        if (m.find()) {
            String release = m.group(1).trim();
            Pattern yearPattern = Pattern.compile("(\\d{4})");
            Matcher ym = yearPattern.matcher(release);
            if (ym.find()) metadata.put("year", ym.group(1));
        }

        return metadata;
    }
}
