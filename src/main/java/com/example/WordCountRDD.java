package com.example;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCountRDD {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("WordCountRDD")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("input.txt");
        JavaRDD<String> words = lines
                .flatMap(line -> Arrays.asList(line.replaceAll("[^a-zA-Z ]", "").toLowerCase().split(" ")).iterator());
        JavaPairRDD<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .mapToPair(Tuple2::swap);

        wordCounts.collect().forEach(System.out::println);

        sc.close();
    }

}
