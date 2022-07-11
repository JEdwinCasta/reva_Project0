package com

object main {

  def main(args: Array[String]): Unit = {
    val login = new Login_system
    val hive = new HiveSparkQueries

    login.login_system()

    val story =
      """The Mall customers dataset holds the details about people visiting the mall.
        |The dataset has an age, customer id, gender, annual income, and spending score.
        |It gains insights from the data and divides the customers into different groups
        |based on their behaviors.""".stripMargin
    println()
    println(story)

    printMenu()

    def printMenu(): Unit = {
      printQuestions()
      Iterator.continually(scala.io.StdIn.readLine("select option: "))
        .takeWhile(_ != "8")
        .foreach{
          case "1" => hive.hiveSQLQueries(1)
            printQuestions()
          case "2" => hive.hiveSQLQueries(2)
            printQuestions()
          case "3" => hive.hiveSQLQueries(3)
            printQuestions()
          case "4" => hive.hiveSQLQueries(4)
            printQuestions()
          case "5" => hive.hiveSQLQueries(5)
            printQuestions()
          case "6" => hive.hiveSQLQueries(6)
            printQuestions()
          case "7" => val trend =
            """1. We can find that women spend slightly more than men and their score are higher
              |2. We can see that the store Brooks and Brothers Factory offers interesting products
              |   for men and women.
              |3. We can find that people between 18 to 36 earn more money than others and therefore,
              |   they tend to visit more frequently
              |""".stripMargin
            println()
            println(trend)
            printQuestions()
          case _  => println("Incorrect option, Please try again")
            printQuestions()
        }
    }

    def printQuestions(): Unit = {
      println()
      println("VIEW CUSTOMER DATA ANALYSIS")
      println()
      println("1. How many visits does every store have?")
      println("2. What is the highest spending score between genders and average age?")
      println("3. Which is the most visited store by gender?")
      println("4. What is the maximum annual income and the minimum annual income in three different age groups (18-36, " +
        "37-55 and 56-74)? ")
      println("5. What are the total visits to the mall in the three different age groups (18-36, " +
        "37-55 and 56-74)? ")
      println("6. What is the average salary between genders?")
      println("7. Trend Analysis ")
      println("8. Exit the program")
    }
    println("Good Bye... ")
  }

}
