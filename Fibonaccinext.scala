package com.my.scala;
import scala.collection.mutable.Set;
import scala.collection.mutable.Map;


object    C {
  
  def main(args:Array[String])
  {
  
  var m =  Map((1 -> "A"),( 2 -> "B"));
  println(m(1))
  println(m(2))
  m.getOrElse(1,"key not found");
  val s = Set(1,2,3,4);
  val tup = (4,"four",4.0,4.0f);
  tup._3;
  
  val l = (1 to 100).toList;
  
  //val res = l.filter(x%2==0).map(u=>u*u).reduce((a,b)=>(a+b));
    
    
  
  
  
  
    }
    
} 
    
  
 
  
  
  