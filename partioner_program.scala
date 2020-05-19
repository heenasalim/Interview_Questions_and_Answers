package info1
import org.apache.spark.{SparkConf.SparkContext,HashPartitioner}
import org.apache.spark.rdd.RDD.rddToPairRDDFuctions
import scala.Iterator

object partioner_program {
 
 
  def main(args:Array[String]):Unit  =   
  {
    
    val cfg =new SparkConf().setAppName("test Partitioning Program").setMaster(Local[*);
    val sc  = new SparkContext(cfg);
    val data = for {  x <- 1 to 4 ; y <- 1 to 6 }
    yield(x,y)
    println("Test Data")
    data.foreach(println)
    println("Creating paired RDD with 2 Hash partitions ");
    val paired = sc.parallelize(data).partitionBy(new hashPartition(2));
    
    
    
    
    
    
    
    
  } 
  
  
  
  
}
}