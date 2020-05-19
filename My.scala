package com.my.scala

object My {
  def main(args:Array[String]):Unit={
    
   for (x <- 5 to 1)  
    println(x + ":  " + factorial(x))
  
  }
  

 def factorial(x: BigInt): BigInt =
    if (x == 0)
      1
    else
      x * factorial(x - 1)
      
      println(factorial(5))

    }

  
