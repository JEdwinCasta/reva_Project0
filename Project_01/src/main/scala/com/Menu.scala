package com


class Menu {

  def printMenu(): Unit = {
    Iterator.continually(scala.io.StdIn.readLine("type here: "))
    .takeWhile(_ != "x")
    .foreach{
      case "this" => println("this")
      case "that" => println("that")
      case _      => println("any")
    }
  }
}
