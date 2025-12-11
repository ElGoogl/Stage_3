package com.indexer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class MetaDataParser{

    private class DBUtil {
        private static final String URL = "jdbc:mysql://localhost:3306/gutenberg_new";
        private static final String USER = "pythonuser";
        private static final String PASSWORD = "twoje_haslo";

        public static Connection getConnection() throws SQLException {
            String url = System.getenv("DB_URL");
            String user = System.getenv("DB_USER");
            String password = System.getenv("DB_PASSWORD");
            return DriverManager.getConnection(URL, USER, PASSWORD);
        }
    }

    public static boolean storeMetadata(Map<String, String> metadata, long bookId) {
        String sql = """
            INSERT INTO book_metadata (
                book_id,
                title,
                author,
                language,
                publication_year
            )
            VALUES (?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                author = VALUES(author),
                language = VALUES(language),
                publication_year = VALUES(publication_year)
        """;

        try (Connection conn = DBUtil.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, bookId);
            stmt.setString(2, metadata.getOrDefault("title", null));
            stmt.setString(3, metadata.getOrDefault("author", null));
            stmt.setString(4, metadata.getOrDefault("language", null));

            if (metadata.containsKey("publication_year")) {
                stmt.setInt(5, Integer.parseInt(metadata.get("publication_year")));
            } else {
                stmt.setNull(5, java.sql.Types.INTEGER);
            }

            stmt.executeUpdate();
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
        Pattern originalPattern = Pattern.compile("^Original publication:\\s*(.+)$", Pattern.MULTILINE);

        Matcher m;

        m = titlePattern.matcher(text);
        if (m.find()) metadata.put("title", m.group(1).trim());

        m = authorPattern.matcher(text);
        if (m.find()) metadata.put("author", m.group(1).trim());

        m = languagePattern.matcher(text);
        if (m.find()) metadata.put("language", m.group(1).trim());

        // Original publication loks like city|country: publisher, year 
        m = originalPattern.matcher(text);
        if (m.find()) {
            String original = m.group(1).trim();
            Pattern yearPattern = Pattern.compile("(\\d{4})");
            Matcher ym = yearPattern.matcher(original);
            if (ym.find()) metadata.put("publication_year", ym.group(1));
        }

        return metadata;
    }
}
