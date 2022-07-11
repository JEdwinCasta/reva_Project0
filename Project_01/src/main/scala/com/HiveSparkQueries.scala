package com
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Scanner

class HiveSparkQueries {

   /* I have created a spark session
   for Windows and one for my HDFS*/
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val scanner = new Scanner(System.in)
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")

  /* - HDFS session
  * - Customers dataframe*/
  val dfc = spark.read.csv("hdfs://localhost:9000/user/hive/warehouse/Mall_Customers1.csv")
  val dfcustomers = dfc.withColumnRenamed("_c0", "custid").withColumn("custid",
      col("custid").cast(IntegerType))
    .withColumnRenamed("_c1", "gender")
    .withColumnRenamed("_c2","age").withColumn("age", col("age").cast(IntegerType))
    .withColumnRenamed("_c3", "annual_income_k").withColumn("annual_income_k",
    col("annual_income_k").cast(IntegerType))
    .withColumnRenamed("_c4", "score").withColumn("score", col("score").cast(IntegerType))
    .withColumnRenamed("_c5", "storeid")
  dfcustomers.write.mode(SaveMode.Overwrite).option("path", "hdfs://localhost:9000/user/hive/warehouse/customers")
    .saveAsTable("customers")

  /*Stores dataframe*/
  val dfs = spark.read.csv("hdfs://localhost:9000/user/hive/warehouse/stores.csv")
  val dfstores: DataFrame = dfs.withColumnRenamed("_c0", "storeid")
                    .withColumnRenamed("_c1", "name")
  dfstores.write.mode(SaveMode.Overwrite).option("path", "hdfs://localhost:9000/user/hive/warehouse/stores")
    .saveAsTable("stores")


  println("created spark session")

  def hiveSQLQueries(x: Int): Unit = {
    try{
      spark.sql("DROP TABLE IF EXISTS customers_part")
      spark.sql("CREATE TABLE customers_part(custid int, age int, annual_income_k int, score int, " +
        "storeid String) partitioned by (gender String) CLUSTERED BY (custid) INTO 10 BUCKETS STORED AS ORC;")

      spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('hive.support.concurrency'='true');")
      spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('hive.txn.manager'='org.apache.hadoop.hive.ql.lockmgr.DbTxnManager');")
      spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('hive.enforce.bucketing'='true');")
      spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('hive.exec.dynamic.partition.mode'='nostrict');")
      spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('transactional'='true');")

      /*Implementing Bucketing and partition */
      spark.sql("INSERT OVERWRITE TABLE customers_part PARTITION(gender) SELECT custid, age, annual_income_k, " +
        "score, storeid, gender FROM customers;")

      if( x == 1) {
        println()
        println("Store visits")
        spark.sql("SELECT s.name, count(*) as visits " +
          "FROM stores s " +
          "join customers_part c On (s.storeid=c.storeid) " +
          "GROUP BY s.name Order by visits DESC;").show(false)
      }else if( x==2){
        println()
        println("Highest Score and Average Age")
        spark.sql("SELECT gender, ROUND(AVG(age)) as Age, ROUND(AVG(score),2) as Highest_Score " +
          "FROM customers_part " +
          "GROUP BY gender ORDER BY Highest_Score DESC;").show()
      }else if(x == 3){
        println()
        println("The most visited store by gender")
        spark.sql("SELECT name, gender, count(gender) AS Total_Visits  " +
          "FROM customers_part c " +
          "JOIN stores s ON (s.storeid = c.storeid) " +
          "GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 2 ;").show(false)
      }else if(x==4){
        println()
        println("The maximum annual income and the minimum annual income for (18-36, " +
          "37-55 and 56-74) ")
        spark.sql("SELECT " +
          "CASE " +
          "WHEN age BETWEEN 18 AND 36 THEN '18 TO 36' " +
          "WHEN age BETWEEN 37 AND 55 THEN '37 TO 55' " +
          "WHEN age BETWEEN 56 AND 74 THEN '56 TO 74' " +
          "END AS AGE_GROUP, " +
          "MAX(annual_income_k) AS MAX_AnnualIncome, " +
          "MIN(annual_income_k) AS MIN_AnnualIncome " +
          "FROM customers_part " +
          "GROUP BY CASE " +
          "WHEN age BETWEEN 18 AND 36 THEN '18 TO 36' " +
          "WHEN age BETWEEN 37 AND 55 THEN '37 TO 55' " +
          "WHEN age BETWEEN 56 AND 74 THEN '56 TO 74' " +
          "END " +
          "ORDER BY AGE_GROUP").show()
      }else if(x == 5){
        println()
        println("The total visits to the mall in for age groups (18-36, " +
          "37-55 and 56-74) ")
        spark.sql("SELECT " +
          "CASE " +
          "WHEN age BETWEEN 18 AND 36 THEN '18 TO 36' " +
          "WHEN age BETWEEN 37 AND 55 THEN '37 TO 55' " +
          "WHEN age BETWEEN 56 AND 74 THEN '56 TO 74' " +
          "END AS AGE_GROUP, " +
          "COUNT(age) AS Total_Visits " +
          "FROM customers_part " +
          "GROUP BY CASE " +
          "WHEN age BETWEEN 18 AND 36 THEN '18 TO 36' " +
          "WHEN age BETWEEN 37 AND 55 THEN '37 TO 55' " +
          "WHEN age BETWEEN 56 AND 74 THEN '56 TO 74' " +
          "END " +
          "ORDER BY AGE_GROUP").show()
      }else{
        println("The average salary between genders")
        spark.sql("SELECT gender, ROUND(AVG(annual_income_k),2) AS Average_Annual_Income_K " +
          "FROM customers_part GROUP BY gender").show()
      }
    }
    catch{
      case e: Exception => e.printStackTrace()
    }
  }
}

