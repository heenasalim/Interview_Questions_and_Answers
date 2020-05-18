
import org.apache.spark.SparkConf;


import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import java.sql.Date
import java.sql.Timestamp


object Heena_Project_Practice {
 
case class smith__idrp_future_reorder_instructions_stores_load_schema(store_location_nbr:String,network_distribution_cd:String,processing_ts:java.sql.Timestamp,source_directive_id:String,source_directive_dt1_nbr:String,source_item_id:String,item_id:String,vendor_package_id:String,reorder_method_cd:String,source_dt:String,source_begin_dt:java.sql.Date,source_end_dt:java.sql.Date,source_directive_type_cd:String,model_priority_nbr:String,servicing_dc_location_nbr:String,dc_effective_dt:String,inforem_ind:String);
val encoder = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[smith__idrp_future_reorder_instructions_stores_load_schema]
case class work__idrp_store_servicing_dc_load_schema(store_location_nbr:String,network_distribution_cd:String,processing_ts:java.sql.Timestamp,source_directive_id:String,source_directive_dt1_nbr:String,source_item_id:String,item_id:String,vendor_package_id:String,reorder_method_cd:String,source_dt:String,source_begin_dt:java.sql.Date,source_end_dt:java.sql.Date,source_directive_type_cd:String,model_priority_nbr:String,servicing_dc_location_nbr:String,dc_effective_dt:String,inforem_ind:String);
case class smith__idrp_vend_pack_dc_combined_load_schema(ksn_package_id:String,ksn_pack_purchase_status_cd:String,substitution_eligible_ind:String,
    dc_stock_ind:String,dc_handling_cd:String,vendor_management_inventory_cd:String,
    ship_aprk_id:String,ship_duns_nbr:String,ship_aprk_type_cd:String,import_cd:String);
 
var work__idrp_store_servicing_dc_load_1_schema = StructType(
                                                    Array(
  StructField("store_location_nbr",StringType,true)
, StructField("network_distribution_cd",StringType)
, StructField("processing_ts",StringType)
, StructField("source_directive_id",StringType)
, StructField("source_directive_dt1_nbr",StringType)
, StructField("source_item_id",StringType)
, StructField("item_id",StringType)
, StructField("vendor_package_id",StringType)
, StructField("reorder_method_cd",StringType)
, StructField("source_dt",StringType)
, StructField("source_begin_dt",DateType)
, StructField("source_end_dt",DateType)
,StructField("source_directive_type_cd",StringType)
,StructField("model_priority_nbr",StringType)
,StructField("servicing_dc_location_nbr",StringType)
,StructField("dc_effective_dt",StringType)
,StructField("inforem_ind",StringType)
          
      
      )) 

  
 def main(args:Array[String]) {
   
  println("Heena");
 val ss = SparkSession.builder().appName("simple project").master("local").getOrCreate();
 import ss.implicits._
 

var work__idrp_store_servicing_dc_load_ds1 = ss.read.
option("header", true)
.option("inferschema", true)
.csv("C://Users/jabin/Desktop/2020_project_files/work_idrp_store_servicing_dc_load.txt")
  .as[smith__idrp_future_reorder_instructions_stores_load_schema].show()
  
  
 var work__idrp_store_servicing_dc_load_ds2 = ss.read.
schema(work__idrp_store_servicing_dc_load_1_schema).
csv("C://Users/jabin/Desktop/2020_project_files/work_idrp_store_servicing_dc_load_without_header.txt")
.as[work__idrp_store_servicing_dc_load_schema]
 
 
 //in order to create dataset from csv dataframe ,dataframe should have schema already as above
 
 
 var work__idrp_store_servicing_dc_load_df = ss.read
 .option("header",true)
 .option("inferschema",true)
 .csv("C://Users/jabin/Desktop/2020_project_files/work_idrp_store_servicing_dc_load.txt")


 /*var header_work = work__idrp_store_servicing_dc_load.first;
var work__idrp_store_servicing_dc = work__idrp_store_servicing_dc_load
.filter(x => x != header_work)
.map(x=> x.split(","))
*/


 var smith__idrp_future_reorder_instructions_stores_load_df = ss.read
 .option("header", true)
  .option("inferschema", true)
 .csv("C://Users/jabin/Desktop/2020_project_files/smith_idrp_future_reorder_instructions_stores_load.txt")

 
 var smith__idrp_future_reorder_instructions_stores_load_ds = ss.read
  .option("header", true)
  .option("inferschema", true)
  .csv("C://Users/jabin/Desktop/2020_proje,,ct_files/smith_idrp_future_reorder_instructions_stores_load.txt")
  .as[smith__idrp_future_reorder_instructions_stores_load_schema];

 
 
 
 import org.apache.spark.sql.types.IntegerType
 
var future_reorder_instructions_stores_leftouter_store_servicing_dc = 
    smith__idrp_future_reorder_instructions_stores_load_df.as('a).join(work__idrp_store_servicing_dc_load_df.as('b),
$"a.servicing_dc_location_nbr" === $"b.servicing_dc_location_nbr" &&  $"a.vendor_package_id" === $"b.vendor_package_id", "left_outer").selectExpr(
     "a.store_location_nbr",
       "a.network_distribution_cd",
        "a.processing_ts",
        "a.source_directive_id",
         "a.source_directive_dt1_nbr",
         "cast(a.source_item_id as Int) as source_item_id",
         "cast(a.item_id as Int) as item_id ",
         "a.vendor_package_id  as vendor_package_id",
         "a.reorder_method_cd as reorder_method_cd",
        "trim(a.source_dt) as source_dt",
        "trim(a.source_begin_dt) as source_begin_dt",
        "trim(a.source_end_dt) as source_end_dt",
        "a.source_directive_type_cd",
        "a.model_priority_nbr",
        "a.servicing_dc_location_nbr",
        "a.inforem_ind"
    )
.select(
     
  $"a.store_location_nbr",
       $"a.network_distribution_cd",
        $"a.processing_ts",
        $"a.source_directive_id",
        $"a.source_directive_dt1_nbr",
        $"source_item_id".cast(IntegerType).alias("souce_item_id"),
        $"item_id",
        $"vendor_package_id",
        $"reorder_method_cd",
        $"source_dt" as "new_source_dt",
        $"source_begin_dt" as "source_begin_dt",
        trim($"source_end_dt") as "source_end_dt",
        $"source_directive_type_cd",
       $"a.model_priority_nbr",
        $"a.servicing_dc_location_nbr",$"a.inforem_ind"
  
  )
 .withColumn("servicing_dc_location_nbr",when($"servicing_dc_location_nbr"  =!= ' ' ||  $"servicing_dc_location_nbr".isNotNull
     , $"servicing_dc_location_nbr"
 
 )
 
 .otherwise(" ")
 ).
 
 withColumn("source_begin_dt",
    when(!($"source_begin_dt".isNull),$"source_begin_dt").otherwise(' ')
    )
   
 .withColumn("inforem_ind", $"inforem_ind"*2 )
 .withColumn("new_inforem_ind", $"inforem_ind"*3)
 .withColumnRenamed("new_inforem_ind","additinal_column") 
 .drop("additional_column")
 .filter( $"source_begin_dt" === "11/01/2020" && $"vendor_package_id" === 'Y' )
 .distinct()
 
 var no_stores_block_logic_sears =
 future_reorder_instructions_stores_leftouter_store_servicing_dc.filter($"source_begin_dt" < $"source_end_dt" ||
 $"source_begin_dt" < $"source_end_dt"     
 
 )
 
 var apply_stores_bock_logic_sears =
  future_reorder_instructions_stores_leftouter_store_servicing_dc.filter(!( $"source_begin_dt" < $"source_end_dt" ||
     $"source_begin_dt" === $"source_end_dt"  || $"source_dt" === "12/01/2020"))
 
apply_stores_bock_logic_sears.groupBy($"source_begin_dt",$"source_end_dt")
.agg(
    
  
    //store_location_nbr,network_distribution_cd,processing_ts,source_directive_id,source_directive_dt1_nbr,source_item_id,item_id,vendor_package_id,reorder_method_cd,source_dt,source_begin_dt,source_end_dt,source_directive_type_cd,model_priority_nbr,servicing_dc_location_nbr,dc_effective_dt,inforem_ind
    collect_list($"store_location_nbr" ) as "store_location_nbr",
    collect_list($"network_distribution_cd") as "network_distribution_cd",
    collect_list($"item_id") as "item_id",
    collect_list($"processing_ts") as "processing_ts"
    
    ).select(from_unixtime( unix_timestamp($"source_begin_dt","dd/MM/yyyy"),"dd/MM/yyyy"),
    from_unixtime(unix_timestamp(current_timestamp(),"MM/dd/yyyy HH:mm:ss aaa"),"MM/dd/yyyy  HH:mm:ss aaa") ,
    $"processing_ts",
    $"network_distribution_cd" as "network_distribution_cd",
    $"item_id" as "item_id",
    $"store_location_nbr" as "store_location_nbr" )
      


 //craeted dataset
 
 smith__idrp_future_reorder_instructions_stores_load_ds.as('b).join( work__idrp_store_servicing_dc_load_ds2.as('a),  
 $"a.servicing_dc_location_nbr" === $"b.servicing_dc_location_nbr","left_outer")
 .groupBy($"a.source_begin_dt",$"a.source_end_dt")
 .agg(
     
  collect_list($"a.store_location_nbr") as "store_location_nbr",
  collect_list($"a.network_distribution_cd") as "network_distribution_cd",
  collect_list($"a.processing_ts") as "processing_ts",
  collect_list($"a.source_directive_id") as "source_directive_id",
  collect_list($"a.source_directive_dt1_nbr") as "source_directive_dt1_nbr",
  collect_list($"a.source_item_id") as "source_item_id",
  collect_list($"a.item_id") as "item_id",
  collect_list($"a.vendor_package_id")  as "vendor_package_id",
  collect_list($"a.reorder_method_cd") as "reorder_method_cd",
  collect_list($"a.source_dt") as "source_dt"
 )
 //
// .withColumn("item_id",when ($"item_id" =!= "000","item_id").otherwise(""))
 .withColumnRenamed("source_location_nbr","source_location_nbr_new")
 //.filter($"store_location_nbr".unzip === "11" )
 

 

 var smith__idrp_vend_pack_dc_combined_load = ss.read
 .option("header", true)
  .option("inferschema", true)
 .csv("C://Users/jabin/Desktop/2020_project_files/smith_idrp_vend_pack_dc_combied_location.txt")
 .as[smith__idrp_vend_pack_dc_combined_load_schema]
 .registerTempTable("smith__idrp_vend_pack_dc_combined_load_table")
 
 ss.sqlContext.sql(" select case when ksn_package_id = 0 then '' else ksn_package_id end as ksn_package_id from smith__idrp_vend_pack_dc_combined_load_table")
 .show()

 
 
 

 
 
 
 
 
 
 
 } 
}



