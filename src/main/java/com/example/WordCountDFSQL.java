package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WordCountDFSQL {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("WordCountDFSQL")
                .master("local")
                .getOrCreate();

        spark.read().text("input.txt").toDF("line").createOrReplaceTempView("lines");

        Dataset<Row> wordCounts = spark.sql(
                "SELECT word, COUNT(*) as count " +
                "FROM (SELECT explode(split(lower(regexp_replace(line, '[^a-zA-Z ]', '')), ' ')) as word FROM lines) " +
                "GROUP BY word ORDER BY count DESC");

        wordCounts.show();

        spark.stop();
    }
}
