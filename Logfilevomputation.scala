package info1
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf;



object Logfilevomputation {
  
  def main(args:Array[String]) = 
  {
    
    
    val dp = new SparkConf().setAppName("Logs file reading").setMaster("local[*]");
    val sc = new SparkContext(dp);
    val text = sc.textFile("E://SPARK_1//heena");
    
    
    var rec = text.filter(A => A.startsWith("Error"));
    val message = rec.map(A=>A.split("\t")).map(A=>A(2));
    val language = rec.map(A=>A.split("\t")).map(A=>A(1));
    
    val scala =language.filter(a=>a.contains("scala"));
    val scala_count = scala.count();
    val language_name = message;
    val message_name  = scala.collect();
    
   
   // val m =language.filter(a=>a.contains("mysql")).count();
    //val H = language.filter(a=>a.contains("Hadoop")).count();
    //val j = language.filter(a=>a.contains("java")).count();
    
    
    println("The error in the " +  language_name +  " :" + message_name +  " "   + scala_count  + "times"  );
 
 
    
    
  }
  
}