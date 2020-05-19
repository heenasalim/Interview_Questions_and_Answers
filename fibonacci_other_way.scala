package com.my.scala

object fibonacci_other_way { 
  
  def main(args:Array[String]) : Unit = {
   
   var a = 0;
   var b = 1;
   var n = 10;
   var i = 0;
  
   while( i < n ) // number iteration second number ie.1
   {
   var res = a + b;     // 0+1   1
   a = b;       //1 
   b = res;      //   
   i = i + 1;
   println(a);
   }
   
}
  }
