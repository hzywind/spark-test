import os
from delta import *
from pyspark.sql import SparkSession

with SparkSession.builder.appName("MyApp").master("local[*]")\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.local.dir", "target/spark/local")\
    .config("spark.jars", "target/spark/jars/h2-2.3.230.jar")\
    .config("spark.driver.extraClassPath", "target/spark/jars/h2-2.3.230.jar")\
    .enableHiveSupport()\
    .getOrCreate() as spark:

    sql = "select o.customer_id, c.name, count (*) order_count from orders o join customers c on o.customer_id = c.id group by o.customer_id, c.name"
    spark.sql(sql).show()
    
    jvm = spark._sc._gateway.jvm
    driver_manager = jvm.java.sql.DriverManager
    conn = driver_manager.getConnection("jdbc:h2:mem:testdb", "sa", "")
    
    
    print(conn)
