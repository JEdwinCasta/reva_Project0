package com
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Scanner
import scala.io.Source


class HiveSparkQueries {

  // create a spark session
  // for Windows
  System.setProperty("hadoop.home.dir", "C:\\hadoop")
  val spark = SparkSession
    .builder
    .appName("hello hive")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  Logger.getLogger("org").setLevel(Level.ERROR)
  spark.sparkContext.setLogLevel("ERROR")
  println("created spark session")

  def login_system(): Boolean = {
    val scanner = new Scanner(System.in)
    var authen = false
    try{
      val pass = Source.fromFile("C:\\Users\\Edwin Castano\\Desktop\\Git Folder" +
        "\\revature\\Project_01\\src\\main\\scala\\com\\password.txt").getLines().mkString
      val url = "jdbc:mysql://localhost:3306/login_system"
      val user = "root"
      val dbtable = "users"


      // now you can use the decrypted value to authenticate to external services
      val sourceDf = spark.read.format("jdbc").option("url", url)
        .option("dbtable", dbtable).option("user", user)
        .option("password", p).load()
      //      sourceDf.show()
      sourceDf.createOrReplaceTempView("users1")



      do{
        println("Enter Username: ")
        val username = scanner.nextLine()
        println("Enter Password: ")
        val pword = scanner.nextLine()
        println("Enter User Type: ")
        val usertype = scanner.nextLine()

        val df = spark.sql(
          s"""SELECT * FROM users1 WHERE username = '${username}'
             | and p4ssword = '${pword}' and usertype = '${usertype}'""".stripMargin)
        //      df.show()

        import spark.implicits._
        val sys_user = df.select("username").map(f=>f.getString(0)).collect().toList
        //    println(sys_user(0))
        if (sys_user.length == 0){
          println("Sorry but the username or password is incorrect. Please try again")
        }
        else{
          println(s"Hello ${sys_user(0).toUpperCase}")
          authen = true
        }
      }while(!authen)

    }
    catch
    {
      case x: Exception => println(x)
    }
    return authen
  }

  def hiveSQLQueries(): Unit = {

    spark.sql("DROP TABLE IF EXISTS customers")
    spark.sql("CREATE TABLE customers(custid int, gender String, age int, annual_income_k int, score int, " +
      "storeid String) row format delimited fields terminated by ','stored as textfile;")
    spark.sql("LOAD DATA LOCAL INPATH 'Mall_Customers.csv' OVERWRITE INTO TABLE customers")
    //    spark.sql("SELECT * FROM customers").show()

    spark.sql("DROP TABLE IF EXISTS customers_part")
    spark.sql("CREATE TABLE customers_part(custid int, age int, annual_income_k int, score int, " +
      "storeid String) partitioned by (gender String) CLUSTERED BY (custid) INTO 10 BUCKETS STORED AS ORC;")

    spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('hive.support.concurrency'='true');")
    spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('hive.txn.manager'='org.apache.hadoop.hive.ql.lockmgr.DbTxnManager');")
    spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('hive.enforce.bucketing'='true');")
    spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('hive.exec.dynamic.partition.mode'='nostrict');")
    spark.sql("ALTER TABLE customers_part SET TBLPROPERTIES ('transactional'='true');")

    spark.sql("INSERT OVERWRITE TABLE customers_part PARTITION(gender) SELECT custid, age, annual_income_k, " +
      "score, storeid, gender FROM customers;")
    spark.sql("SHOW PARTITIONS customers_part").show()
    spark.sql("SELECT * FROM customers_part LIMIT 10").show()

    spark.sql("DROP TABLE IF EXISTS stores")
    spark.sql("CREATE TABLE stores(storeid String, name String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" +
      "STORED as textfile;")
    spark.sql("LOAD DATA LOCAL INPATH 'stores.csv' OVERWRITE INTO TABLE stores")
    spark.sql("SELECT * FROM stores LIMIT 10").show(false)

    println("1. How many visits does every store have? list ascending")
    spark.sql("SELECT s.name, count(*) as visits " +
      "FROM stores s " +
      "join customers_part c On (s.storeid=c.storeid) " +
      "GROUP BY s.name Order by visits ASC;").show(false)

    println("2. What is the highest spending score between genders? show average age")
    spark.sql("SELECT gender, ROUND(AVG(age)) as Age, ROUND(AVG(score),2) as Highest_Score " +
      "FROM customers_part " +
      "GROUP BY gender ORDER BY Highest_Score DESC;").show()

    println("3. Which is the most visited store by gender?")
    spark.sql("SELECT name, gender, count(gender) AS Total_Visits  " +
      "FROM customers_part c " +
      "JOIN stores s ON (s.storeid = c.storeid) " +
      "GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 2 ;").show(false)

    println("4. What is the maximum annual income and the minimum annual income in three different age groups (18-36, " +
      "37-55 and 56-74)? ")
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

    println("5. What are the total visits to the mall in the three different age groups (18-36, " +
      "37-55 and 56-74)? ")
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

    println("6. What is the average salary between genders")
    spark.sql("SELECT gender, ROUND(AVG(annual_income_k),2) AS Average_Annual_Income_K " +
      "FROM customers_part GROUP BY gender").show()
  }

}

