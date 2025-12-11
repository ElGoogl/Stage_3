package com.bd.search;

public class RankedBook extends Book {
    private double finalScore;
    private double tfidfScore;
    
    public RankedBook() {}
    
    public RankedBook(Book book) {
        super(book.getBook_id(), book.getTitle(), book.getAuthor(), book.getLanguage(), book.getYear());
        this.finalScore = 0.0;
        this.tfidfScore = 0.0;
    }
    
    // Getters and setters
    public double getFinalScore() { return finalScore; }
    public void setFinalScore(double finalScore) { this.finalScore = finalScore; }
    
    public double getTfidfScore() { return tfidfScore; }
    public void setTfidfScore(double tfidfScore) { this.tfidfScore = tfidfScore; }
}