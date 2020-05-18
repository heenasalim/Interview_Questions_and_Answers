package Heena
 
import org.apache.spark.SparkConf;
import java.util.ArrayList
//import java.io.*

import org.apache.spark.SparkContext;
import scala.collection.immutable.Seq
import scala.collection.mutable.LinkedList
import scala.collection.mutable.ListBuffer
import scala.collection._

//import collection._

//There are five types of collection object
//List    
//Map
//Set
//Queue
//Array
//Vector

//immutable objects -List,Map,TreeMap,HashMap,Set,TreeSet,HashSet,Arrays,queue,priorityqueue
//Mutable objects - LinkedList,Map,TreeMap,HashMap,Set,TreeSet,queue,priorityqueue,Hashset,ArrayBuffer,ListBuffer,ArrayBulider,ListBuilder


object Collections_Program {
  
 
	
	def main(args:Array[String])
	{
  
   var sconf = new SparkConf().setAppName("Practicing Creating RDD,Dataframe,Dataset").setMaster("local")
   .set("spark.driver.allowMultipleContexts","true") .set("deploy_mode", "client")
   .set("spark.logConf", "true") .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    var sc= new SparkContext(sconf);
   
   
	  
	  // List is immutable
   //colletion of same or different type of elements 
	  //we can perform head,tail,isEmpty,reverse,List.concat(l1,l2,List.fill)sum,min,max,distinct,length
    //we cannot add the elements into list one defined
    var list1 = List(3,4,2,6,3,2)
    var list2 = List("Salim","Heena","String","Shaikh") 
    var list3 =  1::2::3::Nil
    var list4 = List.fill(6)("Heena")   //List(heena, heena, heena, heena, heena, heena)
    //will create new list with heena element 6 times
    //first argument is for how many times values to be filled
    //but we cannot calculate values and fill in the list so we use tabulate
    
    var list5 =  List.tabulate(6)( x => x*2)
    //first argument is how many times we want operation
    var list6 = "Heena".toList
    var list7 = ListBuffer(5,6,7).toList
    
    
    println(list1.reverse) ;           // List(6, 2, 4, 3)
    println(List.concat(list1,list2))  // List(3, 4, 2, 6, Salim, Heena, String, Shaikh)
    println(list1.foldRight(0)(_-_));  // -5 operation will end to right side it will operate only for same type of elements
    println(list2.foldLeft("Name:-")(_+_))    // 15 operation will end to left side
    println(list1.isEmpty); //it checks list is empty or not  and return true or false
   
    println() 
 
  
    println(list1.apply(0))  // select elemets of the list from particular index
    println(list1.filter( x => x!=2)); //filtering particular elements
    println(list1.map(x => x*2))
    println(list1.:::(list2))  //join the list 
    println( list1.::(999)) //add  the element in list at begining
    
    println(list1)
    println(list1.drop(3)) // drop 3 from list
    println(list1.dropRight(3))
    println(list1.dropWhile( x => x != 3))
    println(list1.equals(list2))
   
    println( (list1 zip list2).toMap )   //Map(3 -> Salim, 4 -> Heena, 2 -> String, 6 -> Shaikh)
    //zip will produce tuplesof first element of list1 and list2 
    //second element of list1 and list2
    //eg. (3,5),(4,1)(2,"String")(6,7)
    
    println(list1.toSeq)  
    println(list1.toSet)   //set of unique elements 
    println(list1.toString)
    println(list1.toArray)
    println(list1.toVector)
    println(list1.toBuffer)  //created arraybuffer
    
//Output:-
    
/* List(2, 3, 6, 2, 4, 3)
List(3, 4, 2, 6, 3, 2, Salim, Heena, String, Shaikh)
-4
Name:-SalimHeenaStringShaikh
false
List(Heena, Heena, Heena, Heena, Heena, Heena)
List(0, 2, 4, 6, 8, 10)
3
List(3, 4, 6, 3)
List(6, 8, 4, 12, 6, 4)
List(Salim, Heena, String, Shaikh, 3, 4, 2, 6, 3, 2)
List(999, 3, 4, 2, 6, 3, 2)
List(3, 4, 2, 6, 3, 2)
List(6, 3, 2)
List(3, 4, 2)
List(3, 4, 2, 6, 3, 2)
false
Map(3 -> Salim, 4 -> Heena, 2 -> String, 6 -> Shaikh)
List(3, 4, 2, 6, 3, 2)
Set(3, 4, 2, 6)
List(3, 4, 2, 6, 3, 2)
[I@42f3156d
Vector(3, 4, 2, 6, 3, 2)
ArrayBuffer(3, 4, 2, 6, 3, 2)*/
    
   
    // Types of list : linked list and ArrayList
// linked list is list of items those are connected to each other
// it contains element and connection to  other element
  //it does not contain duplicate elements
    
     var linkedlist1 = LinkedList[Int](5,6,7,8,5,9,6)
   // ll.addString("heena")
     
    println(linkedlist1)
    println(linkedlist1.next) //6,7,8
    println(linkedlist1.next.next)  // 7,8
    println( linkedlist1.filter( x => x!=6 )  //5,7,8
  
       )
     def remove_elements( l : LinkedList[Int] , s:Int)
      { 
       for (i <- l)
       {
         
         if( i == s)
         {
        l.elem = l.next.elem;
        l.next = l.next.next;}
        
       }
      }
      
    def add_elements(l : LinkedList[Int],s: Int)
    {
      var linkedlist2  = l;
      var number =  s;
     // linkedlist2.length = linkedlist2.length + 1;
      if( linkedlist2 == Nil || linkedlist2 != Nil)
      {
        
        linkedlist2.elem  = number;
      //  linkedlist2.next  = linkedlist2.
      }
      
    }
    
       
   println("printing original list elements" + linkedlist1 +
       "adding_elements" + add_elements(linkedlist1,7)  + add_elements(linkedlist1,6) +  "=" + linkedlist1  +
           "removing elements" + remove_elements(linkedlist1,5) + linkedlist1
   )
   
 var linkedlist2 = LinkedList(3,2,1)
 var tuple = (3,4,5,6)
  linkedlist1.insert(linkedlist2) //can add other linkedlist
   
//var arrlist = new collection.mutable.Arra
//Arraylist is not member of scala colletion we can implement it by java.util.collectionlist
  
 
  // ArrayList<Integer>  arrlist  = new ArrayList<Integer>(11);
   
  //As list are immutable , we an implemet processing using linkedlist or listbuffer
  // again maintaining elem and next is hard for linkedlist
  //so we use list buffer which is mutable and adding and removing elements in buffer is easy
  //we can change the size of the listbuffer
  //collection of mixed element
  //duplicates are allowed
  
  var lbuffer = ListBuffer("hena","salim",5,2,2) 
  lbuffer += 9
  lbuffer +=8
  lbuffer -= 0
  lbuffer += (1,3,4)
  lbuffer --= Seq(3,3)
  
  println(lbuffer)
  
  //array is mutable  ,
  //collection of indexed,mixed elements
  //we can access elements by its index
  //But we change the element but not its size once declared
  
  
  var array1 = Array(4,5,6,"heena")
  array1(0) = "Apple";
  array1(2) = "Shaik" 
 // lisbuilder not exist 
  
  var array2 = new Array[Any](10)  //when we create empt object it shouldbe initialize
   array2(0) = 5;
  var array3 =  Array.range(20, 100)
  var array4 = Array.tabulate(0)(n =>n*n)
  var array5 = Array.fill(6)("Salim")
  var list0 = List.range(1,13).toArray
  
  
  //sometime we came to know at runtime weneed some more sizes so we can use ArrayBuffer
  //it is mutable collection of  mixed elements
  //we can change the elements as well as size of the Array
  //we can add or remove elemnts if we no index
   
  var arrbuffer = scala.collection.mutable.ArrayBuffer(9,9,5,11,13)
   arrbuffer += 3
   arrbuffer ++= Seq(8,3,4)
   arrbuffer --= Seq(3,4)
   arrbuffer -= 7
   arrbuffer.remove(2)
   arrbuffer.append(3)
   
  
  //How to create wo dimention array
   
   val twoarray = Array.ofDim[Any](3,3)
   println(twoarray)
   println(twoarray(2)(0))
   twoarray(2)(1) =3
   
   for{i <- 0 until 2 
    j <-  0 until 2
    }
  { 
    var temp = 1
    temp  += 2
    twoarray(i)(j) = temp
    println(twoarray(i)(j))  
    
  }
  
  println(twoarray)
  
 ///Maps  collection of key value pair
 //Bydefault Maps are immutable, we can make them mutable by using scala.collectio.muable class
 // there are many types of Map 
 // eg. HashMap ,TreeMap, sortedMap,linkedHashMap
  //if duplcate keys came in Map it update most recent value of the key
	  
	//HashMap  implements Map interface , uses hash code, random order,put method works
  //LinkedHashMap implements Map interface  , uses double linked list , sorted based on insertion order,put method works
  //TreeMap implements Map and sorted Map interface , it uses tree alorithm, sorted based on keys
  //treemap is immuable in nature
  //Sorted Map implements Map interface , sorted based on keys
  
  
  var simplemap = Map( 3 -> "Ajay" , 1 -> "Heena", 2 ->"Ajinkya",1 -> "Manish")  //not sorted
    
  println(simplemap) //Map(3 -> Ajay, 1 -> Manish, 2 -> Ajinkya)
  
  var linkedhashmap = scala.collection.mutable.LinkedHashMap(3 -> "Ajay" , 1 -> "Heena", 2 ->"Ajinkya",1 -> "Manish")
  linkedhashmap.put(5,"Hannan")
  //Map(3 -> Ajay, 1 -> Manish, 2 -> Ajinkya, 5 -> Hannan)
  // soted based on insertion order 
  println(linkedhashmap)   
  
  var hashmap   = scala.collection.mutable.HashMap(3 -> "Ajay" , 1 -> "Heena", 2 ->"Ajinkya",1 -> "Manish")
  println(hashmap)   //Map(2 -> Ajinkya, 1 -> Manish, 3 -> Ajay)
  //random order
  
  var treemap  = scala.collection.immutable.TreeMap(3 -> "Ajay" , 1 -> "Heena", 2 ->"Ajinkya",1 -> "Manish")
  println(treemap)    // Map(1 -> Manish, 2 -> Ajinkya, 3 -> Ajay)  //sorted based on keys
  
   var sortedmap  = scala.collection.SortedMap(3 -> "Ajay" , 1 -> "Heena", 2 ->"Ajinkya",1 -> "Manish")
  println(sortedmap)  //Map(1 -> Manish, 2 -> Ajinkya, 3 -> Ajay) // sorted based on keys 
 
    simplemap += (1 -> "Vidya")
    sortedmap += (1 -> "Vidya")
    linkedhashmap  += (1 -> "Vidya")
    hashmap += (1 -> "Vidya")
    treemap  += (1 -> "Vidya")
   
   
    //  (3 -> "Ajay" , 1 -> "Heena", 2 ->"Ajinkya",1 -> "Manish", 1 -> "Vidya")
    treemap --= List( 1 ,3)   //removes element based on keys only
    treemap -= 1
    treemap ++= List( 1 -> "Vidya",6 -> "Vidya")
    println(treemap)// Map(1 -> Vidya, 2 -> Ajinkya, 6 -> Vidya)
    
    linkedhashmap.put(2, "Vidya")
    hashmap.put(2, "Vidya")
   
    // we can do insert,delete but not update on treemap
    linkedhashmap(1) = "Heena"
    println(linkedhashmap)
   // Map(3 -> Ajay, 1 -> Heena, 2 -> Ajinkya, 5 -> Hannan)
    
    
    for{ (key,value)  <- linkedhashmap }
      println( key + value)
     
      for  {(key,value)  <- linkedhashmap }
      println( value + key)
      
    //  linkedhashmap.foreach(x => println( x._1))  //will print all keys of map object   
    
      println(linkedhashmap)
      linkedhashmap.keysIterator.reduce( (x,y) => x + y)
    
  
      
  //set it collection of unique elemets
  // collection of mixed elements
  //removes duplicate elements automatically
  //Thre are 4 types of set 
      
   //sortedSet- sorted , implements  Set interface TreeSet  - sorted, uses treealogorithm, implemets sorted Set interface,we cannot update treeset
   //HashSet - random order , uses hash coealgoritm , implements Set inteface
   //linkedhashset - insertion order matters ,implements Set interface
      
  
      var set1 = scala.collection.mutable.Set (3,4,"heena",6,"heena")
      set1 += 3;
      println(set1)  //Set(heena, 6, 3, 4)
      set1 ++= Seq(4,4)
      set1 --= Seq(6,6)
      set1 -= 2 
      println(set1)  //Set(heena, 3, 4)
      
      
      var set2 = scala.collection.immutable.TreeSet(3,4,8)
      
      
     
      
      
  
  
	  
	}
	
	
	
}

  
  
  