package com

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.util.Scanner



object login_system {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)


    var username = ""
    var pword = ""
    val scanner = new Scanner(System.in)

    try{

      val url = "jdbc:mysql://localhost:3306/login_system"
      val user = "root"
      val pass = "Sophie&Emma1981"
      val dbtable = "users"


      // now you can use the decrypted value to authenticate to external services
      val sourceDf = spark.read.format("jdbc").option("url", url)
        .option("dbtable", dbtable).option("user", user)
        .option("password", pass).load()
//      sourceDf.show()
      sourceDf.createOrReplaceTempView("users1")

      println("Enter Username: ")
      username = scanner.nextLine()
      println("Enter Password: ")
      pword = scanner.nextLine()

      val df = spark.sql(s"""SELECT * FROM users1 WHERE username = '${username}' and p4ssword = '${pword}'""")
//      df.show()

      import spark.implicits._
      val sys_user = df.select("username").map(f=>f.getString(0)).collect().toList
  //    println(sys_user(0))
      if (sys_user.length == 0){
        println("the username or password is incorrect")
      }
      else{
        println(s"Hello ${sys_user(0).toUpperCase}")
      }
    }
    catch
    {
      case x: Exception => None
    }
  }

}
