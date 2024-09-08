package com.example;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class WordCountDSSQL {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("WordCountDFSQL")
                .master("local")
                .getOrCreate();

        spark.read().text("input.txt").toDF("line").createOrReplaceTempView("lines");

        Dataset<WordCount> wordCounts = spark.sql(
                "SELECT word, COUNT(*) as count " +
                "FROM (SELECT explode(split(lower(regexp_replace(line, '[^a-zA-Z ]', '')), ' ')) as word FROM lines) " +
                "GROUP BY word ORDER BY count DESC").as(Encoders.bean(WordCount.class));

        wordCounts.collectAsList().forEach(System.out::println);

        spark.stop();
    }
    
    public static class WordCount implements Serializable {
        private String word;
        private long count;
    
        // Default constructor
        public WordCount() {}
    
        // Constructor with fields
        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }
    
        // Getters and setters
        public String getWord() {
            return word;
        }
    
        public void setWord(String word) {
            this.word = word;
        }
    
        public long getCount() {
            return count;
        }
    
        public void setCount(long count) {
            this.count = count;
        }
    
        // toString method for easy printing
        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
