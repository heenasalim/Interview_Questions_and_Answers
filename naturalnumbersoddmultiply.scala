package com.my.scala

object naturalnumbersoddmultiply {
  
  def main(args:Array[String]):Unit={
    
   
   val n = (1 to 50).toList
   println(n)
   
   val res = n.filter(x=>x%2 !=0).map(y=>y*2).reduce((a,b)=>a+b)
   println(res)
}
}


// Map is used for the single variable opeation
// reduce is used fo double variable operation

//*fibonacciseries logic