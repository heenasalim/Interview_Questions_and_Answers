package com.my.scala
import scala.collection.mutable.ArrayBuffer


object fibonacci{
def main( args:Array[String]):Unit=
{
   var arr = ArrayBuffer[Int]();
   //println(arr)
   println(arr)
   arr.append(1);
   arr.append(1);
   var j = 0; 
   for(j <-0 until 10)  ////indexing
   {
     println(arr);
     arr.append(arr(j) + arr(j + 1));   
   /// adding the values to the list
   }  
   println(arr)

   
  // println("List elements are " + f);
   }
     
}
     


//112358