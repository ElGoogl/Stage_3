package com.bd.search;

import java.io.Serializable;

/**
 * Represents book metadata extracted from Gutenberg book headers
 */
public class BookMetadata implements Serializable {
    private int bookId;
    private String title;
    private String author;
    private String language;
    private Integer year;
    private String translator;
    private String releaseDate;
    
    public BookMetadata() {}
    
    public BookMetadata(int bookId, String title, String author, String language, Integer year) {
        this.bookId = bookId;
        this.title = title;
        this.author = author;
        this.language = language;
        this.year = year;
    }
    
    // Getters and setters
    public int getBookId() { return bookId; }
    public void setBookId(int bookId) { this.bookId = bookId; }
    
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    
    public String getAuthor() { return author; }
    public void setAuthor(String author) { this.author = author; }
    
    public String getLanguage() { return language; }
    public void setLanguage(String language) { this.language = language; }
    
    public Integer getYear() { return year; }
    public void setYear(Integer year) { this.year = year; }
    
    public String getTranslator() { return translator; }
    public void setTranslator(String translator) { this.translator = translator; }
    
    public String getReleaseDate() { return releaseDate; }
    public void setReleaseDate(String releaseDate) { this.releaseDate = releaseDate; }
}
