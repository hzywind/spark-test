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
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        try {
        
            spark.sql("DROP TABLE IF EXISTS customers");
            spark.sql("DROP TABLE IF EXISTS orders");
            spark.sql("DROP TABLE IF EXISTS monthly_order_stats");

            spark.sql("CREATE TABLE IF NOT EXISTS customers (id INT, name STRING, email STRING) USING delta");
            spark.sql("CREATE TABLE IF NOT EXISTS orders (order_id INT, customer_id INT, order_date STRING, amount DOUBLE) USING delta");
            spark.sql("CREATE TABLE IF NOT EXISTS monthly_order_stats (customer_id INT, month STRING, order_count INT) USING delta");

            // Load data from CSV files with headers into DataFrames
            Dataset<Row> orders = spark.read().option("header", "true").csv("data/orders.csv");
            orders = orders.withColumn("order_id", orders.col("order_id").cast("int"))
                    .withColumn("customer_id", orders.col("customer_id").cast("int"))
                    .withColumn("amount", orders.col("amount").cast("double"));
            Dataset<Row> customers = spark.read().option("header", "true").csv("data/customers.csv");
            customers = customers.withColumn("id", customers.col("id").cast("int"));

            // print the partitions
            System.out.println("===> Orders partitions: " + orders.rdd().getNumPartitions());
            System.out.println("===> Customers partitions: " + customers.rdd().getNumPartitions());
            
            // Write the DataFrames into the tables
            orders.write().format("delta").mode("overwrite").saveAsTable("orders");
            customers.write().format("delta").mode("overwrite").saveAsTable("customers");

            spark.sql("SELECT * FROM customers").show();
            spark.sql("SELECT * FROM orders").show();

            spark.sql("DELETE from customers WHERE id > 50");

            spark.sql("SHOW tables").show();
        
        } finally {
            spark.stop();
        }
    }
    
}
