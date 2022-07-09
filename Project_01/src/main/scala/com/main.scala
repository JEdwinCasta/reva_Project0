package com

object main {

  def main(args: Array[String]): Unit = {
    val hive = new HiveSparkQueries
    val menu = new Menu

    val x = hive.login_system()
    if (x) menu.printMenu()

//    hive.hiveSQLQueries()
  }

}
