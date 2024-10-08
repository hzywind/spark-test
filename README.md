Follow below steps to configure local installed Spark to access the provisioned Delta Lake data. 

1. Download the Delta jars from Maven repo and put the jars to the `jars` folder of the Spark installation directory. For Spark version 3.5.1, download jars of Delta Lake version 3.2.0.

```
https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar
https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar
```

2. Add below configuration to `spark/conf/spark-defaults.conf`.

```
spark.sql.warehouse.dir file:///<project-path>/spark-warehouse
spark.hadoop.javax.jdo.option.ConnectionURL jdbc:derby:;databaseName=<project-path>/metastore_db;create=true
spark.sql.extensions io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog org.apache.spark.sql.delta.catalog.DeltaCatalog
```

3. When encountering the `java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module ...)` error, add `--add-opens java.base/sun.nio.ch=ALL-UNNAMED` to the JVM options. If VS Code is used, add below configuration to `launch.json`.
```json
{
    "type": "java",
    "name": "<application-name>",
    "request": "launch",
    "mainClass": "<main-class>",
    "projectName": "<project-name>",
    "vmArgs": "--add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED"
}
```