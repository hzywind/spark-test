package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Provision {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark SQL Example")
                .enableHiveSupport()
                .master("local[*]") 
                .getOrCreate();

        spark.sql("CREATE TABLE IF NOT EXISTS customers (id INT, name STRING, email STRING)");
        spark.sql("CREATE TABLE IF NOT EXISTS orders (order_id INT, customer_id INT, order_date STRING, amount DOUBLE)");
        spark.sql("CREATE TABLE IF NOT EXISTS monthly_order_stats (customer_id INT, month STRING, order_count INT)");

        // Load data from CSV files with headers into DataFrames
        Dataset<Row> orders = spark.read().option("header", "true").csv("data/orders.csv");
        Dataset<Row> customers = spark.read().option("header", "true").csv("data/customers.csv");

        // print the partitions
        System.out.println("===> Orders partitions: " + orders.rdd().getNumPartitions());
        System.out.println("===> Customers partitions: " + customers.rdd().getNumPartitions());
        
        // Write the DataFrames into the tables
        orders.write().mode("overwrite").saveAsTable("orders");
        customers.write().mode("overwrite").saveAsTable("customers");

        spark.sql("SELECT * FROM customers").show();
        spark.sql("SELECT * FROM orders").show();
        
        spark.stop();
    }
    
}
