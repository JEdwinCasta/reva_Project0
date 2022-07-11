package com

import java.sql.{Connection, DriverManager, Statement}
import java.util.Scanner
import scala.io.Source
/*This class uses Mysql connection to validate users*/
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

    println()
    println("LOGIN SYSTEM, PLEASE ENTER YOUR USERNAME, PASSWORD AND USER TYPE")
    try{
      do{
        println()
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
    var newusername = ""
    var upuserName = 0
    var newpassword = ""
    var updatepass = 0
    printMenu(username)
    try{
      Iterator.continually(scala.io.StdIn.readLine("Select Option: "))
        .takeWhile(_ != "3")
        .foreach {
          case "1" => println("Enter new username: ")
            if(updatepass==1) {
              newusername = scanner.nextLine()
              upuserName = statement.executeUpdate(
                s"""UPDATE $dbtable SET username = '$newusername'
                   | WHERE username = '$username' and p4ssword = '$newpassword'""".stripMargin)
            }else{
              newusername = scanner.nextLine()
              upuserName = statement.executeUpdate(
                s"""UPDATE $dbtable SET username = '$newusername'
                   | WHERE username = '$username' and p4ssword = '$pword'""".stripMargin)
            }
            if(upuserName == 1){
              println("Username has been updated successfully")
            } else{
              println("Update was not successfully")
            }
            printMenu(username)
          case "2" => println("Enter new password: ")
            if(upuserName == 1) {
              newpassword = scanner.nextLine()
              updatepass = statement.executeUpdate(
                s"""UPDATE $dbtable SET p4ssword = '$newpassword'
                   | WHERE username = '$newusername' and p4ssword = '$pword'""".stripMargin)
            }else {
              newpassword = scanner.nextLine()
              updatepass = statement.executeUpdate(
                s"""UPDATE $dbtable SET p4ssword = '$newpassword'
                   | WHERE username = '$username' and p4ssword = '$pword'""".stripMargin)
            }
            if(updatepass == 1) {
              println("Password has been updated successfully")
            }else{
              println("Update was not successfull")
            }
            printMenu(username)
          case _ => println("Incorrect option, Please try again")
            printMenu(username)
        }
      }
    catch
    {
      case e: Exception => e.printStackTrace()
    }
  }
  def printMenu(username: String): Unit ={
    println()
    println(s"WELCOME ${username.toUpperCase()} TO USER MENU")
    println()
    println("1. Update Username ")
    println("2. Update Password ")
    println("3. Data Analysis")
  }
}
