package com

import java.sql.{Connection, DriverManager, Statement}
import java.util.Scanner
import scala.io.Source

class Login_system{
  val pass: String = Source.fromFile("C:\\input_mall\\password.txt").getLines().mkString

  val url = "jdbc:mysql://localhost:3306/login_system"
  val user = "root"
  val dbtable = "users"
  val driver = "com.mysql.cj.jdbc.Driver"
  var connection:Connection = _
  val scanner = new Scanner(System.in)

  Class.forName(driver)
  connection = DriverManager.getConnection(url, user, pass)
  val statement: Statement = connection.createStatement

  def login_system(): Unit = {

    var authen = false
    try{
      do{
        println("Enter Username: ")
        val username = scanner.nextLine()
        println("Enter Password: ")
        val pword = scanner.nextLine()
        println("Enter User Type: ")
        val usertype = scanner.nextLine()

        val rs1 = statement.executeQuery(
          s"""SELECT * FROM $dbtable WHERE username =
             | '$username' and p4ssword = '$pword' and usertype = '$usertype'""".stripMargin)

        if(rs1.next()){
          println(s"Welcome Back ${rs1.getString("username").toUpperCase}")
          authen = true
          userMenu(username, pword)
        }else{
          println("Sorry but the username or password is incorrect. Please try again")
        }

      }while(!authen)

    }
    catch
    {
      case e: Exception => e.printStackTrace()
    }
  }

  def userMenu(username: String, pword: String): Unit = {
    printMenu()
    try{
      Iterator.continually(scala.io.StdIn.readLine("Select Option: "))
        .takeWhile(_ != "3")
        .foreach {
          case "1" => println("Enter new username: ")
            val newusername = scanner.nextLine()
            val upuserName = statement.executeUpdate(s"""UPDATE $dbtable SET username = '$newusername'
                                                        | WHERE username = '$username' and p4ssword = '$pword'""".stripMargin)
            if(upuserName == 1) println("Username has been updated successfully")
            printMenu()
          case "2" => println("Enter new password: ")
            val newpassword = scanner.nextLine()
            val updatepass = statement.executeUpdate(s"""UPDATE $dbtable SET p4ssword = '$newpassword'
                                                        | WHERE username = '$username' and p4ssword = '$pword'""".stripMargin)
            if(updatepass == 1) println(s"Password for user $username has been updated successfully")
            printMenu()
          case _ => println("Incorrect option, Please try again")
            printMenu()
        }
      }
    catch
    {
      case e: Exception => e.printStackTrace()
    }
  }
  def printMenu(): Unit ={
    println()
    println("WELCOME TO USER MENU")
    println()
    println("1. Update Username ")
    println("2. Update Password ")
    println("3. Main Menu")
  }
}
