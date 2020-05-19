package spark_Context_Programs

//Check Below for more information
//https://mapr.com/blog/using-apache-spark-dataframes-processing-tabular-data/

import org.apache.spark.{SparkContext,SparkConf}


case class user_defined_schema(Name:String,Atm_no:Integer,Adreess:String ,Account_Balance:Long);

object TextFile_Operations {
  
  def main(args:Array[String])
  {
    
    
   var configuration = new SparkConf().setAppName("The pipe file operations").setMaster("local");
   var sc = new SparkContext(configuration);
   var ctext = sc.textFile("C:\\Users\\jabin\\Desktop\\sample_file.txt")
   var words = ctext.map( a  => a.split("|")).map( c=> user_defined_schema(c(0),c(1).toInt ,c(2), c(3).toLong))
   
   //words.toDF();
   
   
    
  }
}