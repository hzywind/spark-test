package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Example")
                .enableHiveSupport()
                .master("local[*]")
                .getOrCreate();

        spark.sql("SET spark.sql.files.maxPartitionBytes=50000").show();

        Dataset<Row> orders = spark.sql("SELECT * FROM orders");
        System.out.println("===> Orders partitions: " + orders.rdd().getNumPartitions());
        Dataset<Row> customers = spark.sql("SELECT * FROM customers");
        System.out.println("===> Customers partitions: " + customers.rdd().getNumPartitions());

        Dataset<Row> join = orders.join(customers, orders.col("customer_id").equalTo(customers.col("id"))); // boardcast join
        System.out.println("===> Join partitions: " + join.rdd().getNumPartitions());

        Dataset<Row> result = join.groupBy("customer_id", "name")
                .count()
                .withColumnRenamed("count", "order_count");
        System.out.println("===> Result partitions: " + result.rdd().getNumPartitions());
        
        result.show(Integer.MAX_VALUE);

        spark.stop();
    }
}