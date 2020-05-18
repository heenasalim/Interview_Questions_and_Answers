package spark_Context_Programs

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext

object MyFirstProgram {
  
   	def main(args: Array[String]) { 
  		val master = "local[2]" 
  
 
 		val cfg = new SparkConf().setAppName("WordCountJob").setMaster(master)	 
  		val sc = new SparkContext(cfg) 
  
 
  		val fileName = "file:/media/lines the anonymous function syntax 
 		val words = file.flaux-1/spark-2.0.0-bin-hadoop2.7/README.md" 
		 
  		val file = sc.textFile(fileName) 
  		 
  		// The following line uses the placeholder syntax 
  		//val words = file.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _) 
  		   		// The following line ustMap(lines => lines.split(" ")).map(words => (words, 1)).reduceByKey((a, b) => a + b) 
  		 
 		/* This will collect all the results to the driver in the cluster and print. 
  		   This may lead to out of memory if the data set is big, in that case use 
  		   words.take(100).foreach(println) 
 		*/ 
 		words.collect.foreach(println) 
  		 
  		sc.stop 
		}
  
  
  
  
}



/appl/hdidrp/pig/scripts/item_eligibility/work__idrp_items_vend_packs_can_carry/perform_item_eligibility_work__idrp_items_vend_packs_can_carry.pig

