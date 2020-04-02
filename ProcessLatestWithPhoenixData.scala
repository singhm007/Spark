package com.cox
import java.io.IOException

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.lit
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.md5
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.SaveMode
import org.apache.phoenix.spark._
import java.text.SimpleDateFormat
import java.text.ParseException

object ProcessLatestWithPhoenixData extends App with LazyLogging {
  override def main(args: Array[String]): Unit = {
    logger.info("Inside the main program of ProcessLatestWithPhoenixData: " + args.length)
    try {
      if (args.length == 2) {
        logger.info(args(0) + " : " + args(1))
        val cfgfile = args(0)
        val partition_date = java.time.LocalDate.now
        val notif_id = args(1)
        val spark = SparkSession.builder.master("yarn-cluster").appName("Spark ProcessLatestWithPhoenixData").getOrCreate;
        val sc = spark.sparkContext
        logger.info("The process to compare phoenix data with latest merged data is started ")
        /**
         * Load typesafe's configuration from hdfs file location
         * @param sparkContext
         * @param confHdfsFileLocation
         * @return com.typesafe.config.Config
         */
        def loadHdfsConfig(sparkContext: SparkContext, confHdfsFileLocation: String): Config = {
          logger.info("loadHdfsConfig starts")
          // Array of 1 element (fileName, fileContent)
          val appConf: Array[(String, String)] = sparkContext.wholeTextFiles(confHdfsFileLocation).collect()
          val appConfStringContent = appConf(0)._2
          logger.info("loadHdfsConfig ends")
          ConfigFactory.parseString(appConfStringContent)
        }
        import spark.implicits._
        //Read the conf file and use it applicationConf.getString("key")
        val applicationConf = loadHdfsConfig(sc, cfgfile)
        logger.debug("Master key" + applicationConf.getString("spark.app.master"))
        val coalesceVal: Int = applicationConf.getString("spark.constant.coalesceVal").toInt

        logger.info("start")
        val processedstep1path = applicationConf.getString("spark.hdfs.processedstep1path.data.path") + partition_date
        val processedstep2path = applicationConf.getString("spark.hdfs.processedstep2path.data.path") + partition_date
        val processedstep3path = applicationConf.getString("spark.hdfs.processedstep3path.data.path")
        val processedstep4path = applicationConf.getString("spark.hdfs.processedstep4path.data.path") + partition_date

        val prclatestdata = spark.read.parquet(processedstep1path)
        val prclatest = prclatestdata.drop("CUSTOMER_KEY")
        val fullphdata = spark.read.parquet(processedstep2path)
        val prcPhdata = fullphdata.drop("NOTIF_ID", "EVENT_MILESTONE_DT") //shd read create date and drop to then stitch it back
        val prcPh = prcPhdata.drop("CREATE_DT")

        logger.debug("prclatest :" + prclatest.count())
        logger.info("prclatest is loaded")
        logger.debug("prcPh :" + prcPh.count())
        logger.info("prcPh is loaded")
        fullphdata.createOrReplaceTempView("fullphdata")
        prcPh.createOrReplaceTempView("phtable")
        //validate the date formats
        val strDateformat: SimpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:MM:SS");

        val phoenixdatafull = spark.sql("select CUST_ACCT_NMBR as CUST_ACCT_NMBR,SITE_ID as SITE_ID,EVENT_TYPE as EVENT_TYPE,nvl(EVENT_SUB_TYPE,'') as EVENT_SUB_TYPE,nvl(HOUSE_NMBR,'') as HOUSE_NMBR,nvl(NODE,'') as NODE,nvl(CHILD_NODE,'') as CHILD_NODE,nvl(BILL_TYPE_CODE,'') as BILL_TYPE_CODE,nvl(CUSTOMER_STATUS_CODE,'') as CUSTOMER_STATUS_CODE,nvl(CUSTOMER_CATEGORY_DESC,'') as CUSTOMER_CATEGORY_DESC,nvl(DWELLING_TYPE_DESC,'') as DWELLING_TYPE_DESC,nvl(SCHED_PAR_BUILD_START_DT,'') as SCHED_PAR_BUILD_START_DT,nvl(SCHED_CHILD_CUTOVER_START_DT,'') as SCHED_CHILD_CUTOVER_START_DT,nvl(ACT_CUTOVER_START_DT,'') as ACT_CUTOVER_START_DT,nvl(ACT_CUTOVER_END_DT,'') as ACT_CUTOVER_END_DT,nvl(TICKETID,'') as TICKETID,nvl(CPE_STATUS,'') as CPE_STATUS,nvl(CPE_STATUS_REASON,'') as CPE_STATUS_REASON,nvl(NOTIF_ID,'') as NOTIF_ID,nvl(EVENT_MILESTONE_DT,'') as EVENT_MILESTONE_DT,nvl(CC_NOTES,'') as CC_NOTES from fullphdata")
        logger.info("phoenixdatafull is created")
        logger.debug("phoenixdatafull :::::" + phoenixdatafull.count())
        val phoenixdata = spark.sql("select CUST_ACCT_NMBR as CUST_ACCT_NMBR,SITE_ID as SITE_ID,EVENT_TYPE as EVENT_TYPE,nvl(EVENT_SUB_TYPE,'') as EVENT_SUB_TYPE,nvl(HOUSE_NMBR,'') as HOUSE_NMBR,nvl(NODE,'') as NODE,nvl(CHILD_NODE,'') as CHILD_NODE,nvl(BILL_TYPE_CODE,'') as BILL_TYPE_CODE,nvl(CUSTOMER_STATUS_CODE,'') as CUSTOMER_STATUS_CODE,nvl(CUSTOMER_CATEGORY_DESC,'') as CUSTOMER_CATEGORY_DESC,nvl(DWELLING_TYPE_DESC,'') as DWELLING_TYPE_DESC,nvl(SCHED_PAR_BUILD_START_DT,'') as SCHED_PAR_BUILD_START_DT,nvl(SCHED_CHILD_CUTOVER_START_DT,'') as SCHED_CHILD_CUTOVER_START_DT,nvl(ACT_CUTOVER_START_DT,'') as ACT_CUTOVER_START_DT,nvl(ACT_CUTOVER_END_DT,'') as ACT_CUTOVER_END_DT,nvl(TICKETID,'') as TICKETID,nvl(CPE_STATUS,'') as CPE_STATUS,nvl(CPE_STATUS_REASON,'') as CPE_STATUS_REASON,nvl(CC_NOTES,'') as CC_NOTES from phtable")
        logger.debug("phoenixdata :::::" + phoenixdata.count())
        logger.info("phoenixdata is created")
        val newdiffData = prclatest.except(phoenixdata)
        logger.debug("newdiffData :::::" + newdiffData.count())
        logger.info("newdiffData is created")
        /*val sparkDriver = "org.apache.phoenix.spark"
        val zkconnection = "dvtcbddd01.corp.cox.com,dvtcbddd101.corp.cox.com,dvtcbddd02.corp.cox.com:2181"
        val principal = "bdnodesplit@HDP_DEV.COX.COM"
        val connection = zkconnection + ":" + principal*/

        val updateevt = prclatest.alias("a").join(phoenixdata.alias("b"), prclatest.col("CUST_ACCT_NMBR") === phoenixdata.col("CUST_ACCT_NMBR") && prclatest.col("SITE_ID") === phoenixdata.col("SITE_ID") && prclatest.col("EVENT_TYPE") === phoenixdata.col("EVENT_TYPE"), "left")
          .filter(col("a.EVENT_SUB_TYPE").notEqual(col("b.EVENT_SUB_TYPE"))).select(col("a.*"))
          .withColumn("LAST_MODIFIED_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
          .withColumn("UPDATED_BY_PROCESS", lit("BATCH"))
          .withColumn("EVENT_MILESTONE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("NOTIF_ID", lit(notif_id))

        logger.info("updateevt created")
        logger.debug("updateevt created ::" + updateevt.count())

        val updateevtcdate = updateevt.alias("a").join(prcPhdata.alias("b"), updateevt.col("CUST_ACCT_NMBR") === prcPhdata.col("CUST_ACCT_NMBR") && updateevt.col("SITE_ID") === prcPhdata.col("SITE_ID") && updateevt.col("EVENT_TYPE") === prcPhdata.col("EVENT_TYPE"), "inner").select(col("a.*"), col("b.CREATE_DT")).select($"CUST_ACCT_NMBR", $"SITE_ID", $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE", $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CREATE_DT", $"LAST_MODIFIED_DT", $"UPDATED_BY_PROCESS", $"EVENT_MILESTONE_DT", $"NOTIF_ID",$"CC_NOTES")
        logger.info("updateevtcdate created")
        logger.debug("updateevtcdate : " + updateevtcdate.count())

        val updateevtforh = updateevtcdate.alias("a").join(prclatestdata.alias("b"), updateevtcdate.col("CUST_ACCT_NMBR") === prclatestdata.col("CUST_ACCT_NMBR") && updateevtcdate.col("SITE_ID") === prclatestdata.col("SITE_ID") && updateevtcdate.col("EVENT_TYPE") === prclatestdata.col("EVENT_TYPE"), "inner").select(col("a.*"), col("b.CUSTOMER_KEY")).select($"CUST_ACCT_NMBR", $"SITE_ID", $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE", $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CREATE_DT", $"LAST_MODIFIED_DT", $"UPDATED_BY_PROCESS", $"EVENT_MILESTONE_DT", $"NOTIF_ID", $"CUSTOMER_KEY")
        logger.info("updateevtforh created")
        logger.debug("updateevtforh  :" + updateevtforh.count())

        val updatenonevt = prclatest.alias("a").join(phoenixdatafull.alias("b"), prclatest.col("CUST_ACCT_NMBR") === phoenixdatafull.col("CUST_ACCT_NMBR") && prclatest.col("SITE_ID") === phoenixdatafull.col("SITE_ID") && prclatest.col("EVENT_TYPE") === phoenixdatafull.col("EVENT_TYPE"), "left")
          .filter(col("a.EVENT_SUB_TYPE") === (col("b.EVENT_SUB_TYPE"))).select(col("a.*"), col("b.EVENT_MILESTONE_DT"), col("b.NOTIF_ID"))
          .withColumn("LAST_MODIFIED_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
          .withColumn("UPDATED_BY_PROCESS", lit("BATCH"))
        logger.info("updatenonevt created")
        logger.debug("updatenonevt :" + updatenonevt.count())

        val updatenonevtdate = updatenonevt.alias("a").join(prcPhdata.alias("b"), updatenonevt.col("CUST_ACCT_NMBR") === prcPhdata.col("CUST_ACCT_NMBR") && updatenonevt.col("SITE_ID") === prcPhdata.col("SITE_ID") && updatenonevt.col("EVENT_TYPE") === prcPhdata.col("EVENT_TYPE"), "inner").select(col("a.*"), col("b.CREATE_DT")).select($"CUST_ACCT_NMBR", $"SITE_ID", $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE", $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CREATE_DT", $"LAST_MODIFIED_DT", $"UPDATED_BY_PROCESS", $"EVENT_MILESTONE_DT", $"NOTIF_ID",$"CC_NOTES")
        logger.info("updatenonevtdate created ")
        logger.debug("updatenonevtdate created :" + updatenonevtdate.count())

        val newdata = prclatest.alias("a").join(phoenixdata.alias("b"), prclatest.col("CUST_ACCT_NMBR") === phoenixdata.col("CUST_ACCT_NMBR") && prclatest.col("SITE_ID") === phoenixdata.col("SITE_ID") && prclatest.col("EVENT_TYPE") === phoenixdata.col("EVENT_TYPE"), "left")
          .filter((col("a.CUST_ACCT_NMBR").isNotNull && col("b.CUST_ACCT_NMBR").isNull) && (col("a.SITE_ID").isNotNull && col("b.SITE_ID").isNull) && (col("a.EVENT_TYPE").isNotNull && col("b.EVENT_TYPE").isNull))
          .select(col("a.*")).withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
          .withColumn("LAST_MODIFIED_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
          .withColumn("UPDATED_BY_PROCESS", lit("BATCH"))
        logger.info("newdata created")
        logger.debug("newdata created :" + newdata.count())

        val evtdata = newdata.filter((col("EVENT_SUB_TYPE").isNotNull) && (col("EVENT_SUB_TYPE") !== "")).withColumn("EVENT_MILESTONE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("NOTIF_ID", lit(notif_id)).withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).select($"CUST_ACCT_NMBR", $"SITE_ID", $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE", $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CREATE_DT", $"LAST_MODIFIED_DT", $"UPDATED_BY_PROCESS", $"EVENT_MILESTONE_DT", $"NOTIF_ID",$"CC_NOTES")
        logger.info("evtdata created")
        logger.debug("evtdata :" + evtdata.count())
        val evtdataforh = evtdata.alias("a").join(prclatestdata.alias("b"), evtdata.col("CUST_ACCT_NMBR") === prclatestdata.col("CUST_ACCT_NMBR") && evtdata.col("SITE_ID") === prclatestdata.col("SITE_ID") && evtdata.col("EVENT_TYPE") === prclatestdata.col("EVENT_TYPE"), "inner").select(col("a.*"), col("b.CUSTOMER_KEY")).select($"CUST_ACCT_NMBR", $"SITE_ID", $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE", $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CREATE_DT", $"LAST_MODIFIED_DT", $"UPDATED_BY_PROCESS", $"EVENT_MILESTONE_DT", $"NOTIF_ID", $"CUSTOMER_KEY")
        logger.info("evtdataforh created")
        logger.debug("evtdataforh :" + evtdataforh.count())
        val nonevtdata = newdata.filter(col("EVENT_SUB_TYPE").isNull || col("EVENT_SUB_TYPE") === "").withColumn("EVENT_MILESTONE_DT", lit("")).withColumn("NOTIF_ID", lit("")).withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).select($"CUST_ACCT_NMBR", $"SITE_ID", $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE", $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CREATE_DT", $"LAST_MODIFIED_DT", $"UPDATED_BY_PROCESS", $"EVENT_MILESTONE_DT", $"NOTIF_ID",$"CC_NOTES")
        logger.info("nonevtdata created ")
        logger.debug("nonevtdata :" + nonevtdata.count())

        val dfhist = Seq(evtdataforh, updateevtforh)
        logger.info("dfhist created :")
        val dfhistunion = dfhist.reduce(_ union _)
        logger.info("dfhistunion created ")
        logger.debug("dfhistunion :" + dfhistunion.count())
        val dfhstdata = dfhistunion.withColumn("notif_trans_id", md5(concat_ws(",", $"CUST_ACCT_NMBR", $"SITE_ID", $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"EVENT_MILESTONE_DT", $"UPDATED_BY_PROCESS"))).select(col("notif_trans_id"), col("CUSTOMER_KEY"), col("CUST_ACCT_NMBR"), col("SITE_ID"), col("CHILD_NODE"), col("NODE"), col("EVENT_TYPE"), col("EVENT_SUB_TYPE"), col("EVENT_MILESTONE_DT"), col("CREATE_DT"))
        logger.info("dfhstdata created ")
        logger.debug("dfhstdata :" + dfhstdata.count())
        dfhstdata.coalesce(coalesceVal).write.mode(SaveMode.Append).option("delimiter", "\u0001").csv(processedstep3path)
        logger.info("dfhstdata saved ")
        val dfph = Seq(evtdata, nonevtdata, updateevtcdate, updatenonevtdate)
        val dfphunion = dfph.reduce(_ union _)
        logger.info("dfphunion created ")
        logger.debug("dfphunion :" + dfphunion.count())
        dfphunion.coalesce(coalesceVal).write.mode(SaveMode.Overwrite).parquet(processedstep4path) //TODO write individual df and append
        logger.debug("dfphunion saved :" + dfphunion.count())

      } else {
        logger.info("This program takes minimum 2 parameters.Please pass the property file and optional previous partition date for the first time load." + args.length)
        throw new Exception("This program takes minimum 2 parameters.Please pass the property file and optional previous partition date for the first time load.")
      }
    } catch {
      case noelem: NoSuchElementException =>
        logger.error("Dataframe is empty" + noelem.printStackTrace())
        throw noelem
      case ioe: IOException =>
        logger.error("IOException in ProcessLatestWithPhoenixData" + ioe.printStackTrace())
        throw ioe
      case confexp: ConfigException =>
        logger.error("ConfigException in ProcessLatestWithPhoenixData" + confexp.printStackTrace())
        throw confexp
    }
  }
}