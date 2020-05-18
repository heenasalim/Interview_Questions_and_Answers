import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import collection._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._;
import org.apache.spark.sql.Dataset;

   case class dataschema(subject_namee:String,subject_id:Int,salary:Int);
//either we can use sparksession  or sqlcontext else it will give error while creating dataframe or dataset

object hang {
  
 def main(args:Array[String]) : Unit = {
  
   var sconf = new SparkConf().setAppName("Practicing Creating RDD,Dataframe,Dataset").setMaster("local");
   var sc= new SparkContext(sconf);
   val ss  = SparkSession.builder().appName("DataSet Operation").master("local").getOrCreate();
   import ss.implicits._
   import ss._
   
   //ways to create rdd
    //1> 
   var list1 = List("English",1,220000)
   var rdd1 =  sc.parallelize(list1,3)   //.. .........Created RDD
   //2>
   var text1 =  sc.textFile("C://Users/Jabin/Desktop/delimited_files"); 
  
   //3> from existing rdd
   
   var rdd2 = text1.map(x => x.split("|"));
   
   //4> creating rdd from dataset 
    // println("list2" + list2);    
    //always give schema to object then only toDS() will work
    
    var list3 =  List(dataschema("Science",3,6777),dataschema("oracle",6,2000))  
    var dataset1 = list3.toDS()
    println("Rdd from dataset")
    dataset1.rdd.foreach(println)

  
   //Creating rdd from dataframe
 
     var list4 = List(dataschema("Hadoop",5,200000),dataschema("Scala",9,600000));
     var dataframe1 = list4.toDF();
     
     println("Rdd from dataframe1");
     dataframe1.rdd.foreach(println);
     
  //*****************************************************************************************************//
   //Creating Dataframe
     
    //normal way
     
     //Read how to create dataframe form List
    

    //We cannot create dataframe from list for it you should have schema 
    // then you can use to DF
     
     List(dataschema("Spark",34,60000),dataschema("pig",45,256627)).toDF().show();
 
     
     var seq1 = Seq(1,2,82992);
   
     
     //  creating dataframe using toDF() method
      var schemaseq = Seq("id","name","salary")
     seq1.toDF()
    
     //creating dataframe from RDD
     
       sc.parallelize(seq1).toDF()
     
     //creating dataframe from dataset
       
      var dataset2 =  seq1.toDS()
      var dataframe3 = dataset2.toDF() 
      
   //creating dataframe from different files
      
    var dataframe4 =  ss.read.format("com.databricks.spark.csv").csv("C://Users/Jabin/Desktop/delimited_files/csv.txt")
      
      println(dataframe4);
      
      
      //***********************************************************************************************************//
                                                             
      
      
      
 }
}