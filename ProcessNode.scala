package com.cox

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.streaming.{ ProcessingTime, StreamingQuery, StreamingQueryException}
import scala.Serializable
// import com.fasterxml.jackson.databind.{ JsonMappingException, JsonParseException }
object ProcessNode {

def main(args: Array[String]): Unit = {
val rootLogger = Logger.getRootLogger()
rootLogger.setLevel(Level.INFO)
val spark = SparkSession.builder.appName("ProcessNode").getOrCreate
val sc = spark.sparkContext
import spark.implicits._
val cfgfile = args(0)
val partition_date = args(1)
 def loadHdfsConfig(sparkContext: SparkContext, confHdfsFileLocation: String): Config = {
          rootLogger.info("loadHdfsConfig starts")
          // Array of 1 element (fileName, fileContent)
          val appConf: Array[(String, String)] = sparkContext.wholeTextFiles(confHdfsFileLocation).collect()
          val appConfStringContent = appConf(0)._2
          rootLogger.info("loadHdfsConfig ends")
          ConfigFactory.parseString(appConfStringContent)
        }
		
        //Read the conf file and use it applicationConf.getString("key")
        val applicationConf = loadHdfsConfig(sc, cfgfile)
        rootLogger.debug("Master key" + applicationConf.getString("spark.app.master"))
        val wattspath = applicationConf.getString("spark.hdfs.parquetpath.watts.processed.data.path")
		
val wattsdata = spark.read.parquet(wattspath + partition_date)
val zkconnection = applicationConf.getString("spark.phoenix.bootstrap.servers") 
val principal = applicationConf.getString("spark.phoenix.principal")
val connection = "jdbc:phoenix:" + zkconnection + ":" + principal
rootLogger.info("Connection" + connection)
val sparkDriver = applicationConf.getString("spark.phoenix.driver")
val table = applicationConf.getString("spark.phoenix.table")
rootLogger.info("table" + table)
wattsdata.createOrReplaceTempView("watts")

val parentdata=spark.sql("select par_node as node, site_id,par_node as parent_node,par_bld_strt_dt as sched_current_build_start_dt, par_cutover_strt_dt as sched_current_cutover_start_utc from watts").withColumn("event_type",lit("N+0"))
val childdata= spark.sql("select child_node as node, site_id,chld_bld_strt_dt as sched_current_build_start_dt,chld_cutover_strt_dt as sched_current_cutover_start_utc from watts").withColumn("parent_node",lit(null).cast("string")).withColumn("event_type",lit("N+0"))

val mergedata= parentdata.select("node","site_id","event_type","parent_node","sched_current_build_start_dt","sched_current_cutover_start_utc").union(childdata.select("node","site_id","event_type","parent_node","sched_current_build_start_dt","sched_current_cutover_start_utc")).distinct
rootLogger.info("fetched latest data")

val jdbcDriver = "org.apache.phoenix.jdbc.PhoenixDriver"
  rootLogger.info("jdbcDriver" + jdbcDriver)
val phoenixprev = spark.read.format("jdbc").options(Map("driver" -> jdbcDriver, "url" -> connection, "dbtable" -> table)).load();
rootLogger.info("fetched phoenix data")
mergedata.createOrReplaceTempView("mergedatatable")
phoenixprev.createOrReplaceTempView("phoenixprevtable")

val modifydata = spark.sql("select node,site_id,event_type,parent_node,sched_current_build_start_dt,sched_current_cutover_start_utc from mergedatatable minus select node,site_id,event_type,parent_node,sched_current_build_start_dt,sched_current_cutover_start_utc from phoenixprevtable");

modifydata.createOrReplaceTempView("modifydatatable")

val nodedata= spark.sql("select m.node,m.site_id,m.event_type,m.parent_node,m.sched_current_build_start_dt,m.sched_current_cutover_start_utc,p.sched_prev_build_start_dt,p.sched_prev_cutover_start_dt,p.actual_cutover_dt,NVL(p.CREATE_DT,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss')) as create_dt,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss') as last_modified_dt,'BATCH' as updated_by_process from modifydatatable m left outer join phoenixprevtable p ON p.NODE=m.NODE and p.SITE_ID=m.SITE_ID and p.EVENT_TYPE=m.EVENT_TYPE")

 val nodedatadatatype = nodedata.selectExpr("node","site_id","event_type","parent_node","cast(sched_current_build_start_dt AS TIMESTAMP)","cast(sched_current_cutover_start_utc AS TIMESTAMP)","cast(sched_prev_build_start_dt AS TIMESTAMP)","cast(sched_prev_cutover_start_dt AS TIMESTAMP)","actual_cutover_dt","cast(create_dt AS TIMESTAMP)","cast(last_modified_dt AS TIMESTAMP)","updated_by_process")

nodedatadatatype.write.format(sparkDriver).mode("overwrite").option("driver", jdbcDriver).option("table", table).option("zkUrl", connection).save()

rootLogger.info("loaded data into phoenix table")
}}

