package com.cox

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import java.io.IOException
import com.typesafe.config.ConfigException
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.datediff
import org.apache.spark.sql.functions.current_date
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.substring
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lpad
/*
 * This program joins GNIS,WATTS,CUST_DIM and HOUSE_DIM datas
 * It takes 2 parameters
 * 1) Config name 2) notiif_id which is the unix epochtime prepended with BATCH
 * Merged data is joined with the milestone lookup table to derive event_sub_type value
 * Write the mergered data into node report hdfs path for reporting and
 * processed hdfs path to perform further data processing on phoenix table
 */
object MergeGinsWattsDimReal extends App with LazyLogging {
  override def main(args: Array[String]): Unit = {
    //This generates the merged data and writes into a hdfs folder with date partition
    logger.info("Inside the main program of MergeGinsWattsDimReal: " + args.length)
    try {
      if (args.length == 2) {
        logger.info(args(0) + " : " + args(1))
        val cfgfile = args(0)
        val partition_date = java.time.LocalDate.now
        val notif_id = args(1)
        val spark = SparkSession.builder.master("yarn-cluster").appName("Spark MergeGinsWattsDimReal").getOrCreate;
        val sc = spark.sparkContext
        logger.info("The Merge process is started " + partition_date)
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
        //Read the conf file and use it applicationConf.getString("key")
        val applicationConf = loadHdfsConfig(sc, cfgfile)

        logger.debug("Master key" + applicationConf.getString("spark.app.master"))
        val gnisdatapath = applicationConf.getString("spark.hdfs.parquet.inputgnis.data.path")
        val wattsdatapath = applicationConf.getString("spark.hdfs.parquet.inputwatts.data.path")
        val custdatapath = applicationConf.getString("spark.hdfs.parquet.inputcustparquet.data.path")
        val housedatapath = applicationConf.getString("spark.hdfs.parquet.inputhouseparquet.data.path")
        val milestonelookup = applicationConf.getString("spark.hdfs.milestonelookup.data.path")
        val processedrptpath = applicationConf.getString("spark.hdfs.processedrptpath.data.path")
        val processedstep1path = applicationConf.getString("spark.hdfs.processedstep1path.data.path")
        val coalesceVal: Int = applicationConf.getString("spark.constant.coalesceVal").toInt

        val gnisdata = spark.read.parquet(gnisdatapath + partition_date).filter(col("SITE_ID").isNotNull && col("CHLD_NODE").isNotNull && col("PAR_NODE").isNotNull && col("ACCT_NBR").isNotNull)
        val wattsdata = spark.read.parquet(wattsdatapath + partition_date).filter(col("SITE_ID").isNotNull && col("CHILD_NODE").isNotNull && col("PAR_NODE").isNotNull)
        val custdata = spark.read.parquet(custdatapath).filter(col("SITE_ID").isNotNull && col("ACCOUNT_NBR").isNotNull)
        val housedata = spark.read.parquet(housedatapath).filter(col("SITE_ID").isNotNull && col("HOUSE_NBR").isNotNull)
        if (custdata.head(1).isEmpty || housedata.head(1).isEmpty) {
          throw new Exception("Dimension data is empty")
          System.exit(1)
        }
        import spark.implicits._
        if ((!gnisdata.head(1).isEmpty) && (!wattsdata.head(1).isEmpty) && (!custdata.head(1).isEmpty) && (!housedata.head(1).isEmpty)) {
          val ginsswatts = gnisdata.alias("g").join(wattsdata.alias("w"), gnisdata.col("SITE_ID") === wattsdata.col("SITE_ID") &&
            gnisdata.col("CHLD_NODE") === wattsdata.col("CHILD_NODE") && gnisdata.col("PAR_NODE") === wattsdata.col("PAR_NODE") &&
            gnisdata.col("curr_flg") === "Y" && wattsdata.col("curr_flg") === "Y", "inner")
            .select($"g.ACCT_NBR", $"g.SITE_ID", $"g.ACCT_FLG", $"g.HOUSE_NBR", $"w.CHILD_NODE", $"w.CHLD_BLD_STRT_DT", $"w.CHLD_CUTOVER_STRT_DT", $"w.PAR_NODE", $"w.PAR_CUTOVER_STRT_DT", $"w.PAR_BLD_STRT_DT")
          logger.info("ginsswatts created")
          ginsswatts.show(false)
          val ginsswattscust = ginsswatts.alias("gw").join(custdata.alias("c"), ginsswatts.col("SITE_ID") === custdata.col("SITE_ID") &&
            ginsswatts.col("ACCT_NBR") === custdata.col("ACCOUNT_NBR"), "left").select(col("gw.*"), $"c.CUSTOMER_KEY", $"c.SIC_DESC", $"c.CUSTOMER_STATUS_CD", $"c.CUSTOMER_CATEGORY_CD", $"c.CUSTOMER_CATEGORY_DESC", $"c.CUSTOMER_NM", $"c.REASON_CODE")
          logger.info("ginsswattscust created")
          ginsswattscust.show(false)
          val ginsswattscusthouse = ginsswattscust.alias("gwc").join(housedata.alias("h"), ginsswattscust.col("SITE_ID") === housedata.col("SITE_ID") && ginsswattscust.col("HOUSE_NBR") === housedata.col("HOUSE_NBR"), "left")
            .select(col("gwc.*"), $"h.HOUSE_NBR", $"h.ASSOCIATION_CD", $"h.SIC_CD", $"h.DWELLING_TYPE_CD", $"h.DWELLING_TYPE_DESC", $"h.BILL_TYPE_CD", when(col("ADDRESS_LINE_1").isNotNull, col("ADDRESS_LINE_1")).otherwise(lit("_")).alias("ADDRESS_LINE_1"),
              when(col("ADDRESS_LINE_2").isNotNull, col("ADDRESS_LINE_2")).otherwise(lit("")).alias("ADDRESS_LINE_2"), when(col("ADDRESS_LINE_3").isNotNull, col("ADDRESS_LINE_3")).otherwise(lit("")).alias("ADDRESS_LINE_3"), when(col("ADDRESS_LINE_4").isNotNull, col("ADDRESS_LINE_4"))
                .otherwise(lit("")).alias("ADDRESS_LINE_4")).drop($"gwc.HOUSE_NBR").withColumn("SERVICE_ADDRESS", concat(col("ADDRESS_LINE_1"), lit(" "), col("ADDRESS_LINE_2"), lit(" "), col("ADDRESS_LINE_3"), lit(" "), col("ADDRESS_LINE_4")))
            .drop("ADDRESS_LINE_1", "ADDRESS_LINE_2", "ADDRESS_LINE_3", "ADDRESS_LINE_4").withColumn("clientdiff", datediff(to_date(substring(col("CHLD_CUTOVER_STRT_DT"), 1, 10)), current_date()).cast(IntegerType)).withColumn("parentdiff", datediff(to_date(substring(col("PAR_BLD_STRT_DT"), 1, 10)), current_date()).cast(IntegerType))
          ginsswattscusthouse.cache()
          logger.info("ginsswattscusthouse created")
          ginsswattscusthouse.show(false)
          val milestone = spark.read.format("csv").option("header", "true").load(milestonelookup)
          val milefilter = milestone.select(col("cutover_threshold_low").cast(IntegerType), col("cutover_threshold_high").cast(IntegerType), col("build_threshold_low").cast(IntegerType), col("build_threshold_high").cast(IntegerType), col("event_type"), col("event_sub_type"), col("icoms_cc_notes_txt"))
          milefilter.createOrReplaceTempView("newmile")
          spark.table("newmile").cache
          logger.info("milefilter created")
          val c2c14 = ginsswattscusthouse.filter((col("clientdiff").>=(0) && col("clientdiff").<=(2)) || ((col("clientdiff").>=(3) && col("clientdiff").<=(14))))
          c2c14.createOrReplaceTempView("c2c14t")
          spark.table("c2c14t").cache
          val c2c14data = spark.sql("select b.event_sub_type,b.event_type,b.icoms_cc_notes_txt,a.* from newmile b,c2c14t a where a.clientdiff>=b.cutover_threshold_low and a.clientdiff<=b.cutover_threshold_high")
          logger.info("c2c14data created")
          c2c14data.cache
          val c1530BTC = ginsswattscusthouse.filter(col("clientdiff").>=(15) && col("clientdiff").<=(30) && col("BILL_TYPE_CD").===("C")).withColumn("clientdiff", trim(col("clientdiff")))
          logger.info("c1530BTC created")
          c1530BTC.createOrReplaceTempView("c1530BTCt")
          spark.table("c1530BTCt").cache
          val c1530BTCdata = spark.sql("select b.event_sub_type,b.event_type,b.icoms_cc_notes_txt,a.* from newmile b,c1530BTCt a where ((a.clientdiff>=b.cutover_threshold_low) and (a.clientdiff<=b.cutover_threshold_high))")
          logger.info("c1530BTCdata created")
          c1530BTCdata.cache
          val combinedDf = Seq(c2c14data, c1530BTCdata)
          val dfunion = combinedDf.reduce(_ union _).withColumnRenamed("icoms_cc_notes_txt", "CC_NOTES")
          dfunion.cache()
          if (dfunion.head(1).isEmpty) {
            logger.info("dfunion is null")
            val b2b14 = ginsswattscusthouse.filter((col("clientdiff") > 14 && col("BILL_TYPE_CD").!=("C")) || ((col("clientdiff") > (30) && col("BILL_TYPE_CD").===("C"))))            
            logger.info("b2b14 created")
            b2b14.createOrReplaceTempView("b2b14t")
            spark.table("b2b14t").cache
            val b2b14data = spark.sql("select b.event_sub_type,b.event_type,b.icoms_cc_notes_txt,a.* from newmile b,b2b14t a where ((a.parentdiff>=b.build_threshold_low) and (a.parentdiff<=b.build_threshold_high))")
            val combinedCBDf = Seq(dfunion, b2b14data)
            val dfCBunion = combinedCBDf.reduce(_ union _)
            dfCBunion.cache()
            if (dfCBunion.head(1).isEmpty) {
              logger.info("dfCBunion is empty")
              val fetchotherrecs = ginsswattscusthouse.withColumn("event_type", lit("N+0")).withColumn("event_sub_type", lit("")).withColumn("CC_NOTES", lit(""))
              val ordercolsothers = fetchotherrecs.select(col("event_sub_type"), col("event_type"), col("CC_NOTES"), col("ACCT_NBR"), col("SITE_ID"), col("ACCT_FLG"), col("CHILD_NODE"),
                col("CHLD_BLD_STRT_DT"), col("CHLD_CUTOVER_STRT_DT"), col("PAR_NODE"), col("PAR_CUTOVER_STRT_DT"), col("PAR_BLD_STRT_DT"), col("CUSTOMER_KEY"),
                col("SIC_DESC"), col("CUSTOMER_STATUS_CD"), col("CUSTOMER_CATEGORY_CD"), col("CUSTOMER_CATEGORY_DESC"), col("CUSTOMER_NM"), col("REASON_CODE"),
                col("HOUSE_NBR"), col("ASSOCIATION_CD"), col("SIC_CD"), col("DWELLING_TYPE_CD"), col("DWELLING_TYPE_DESC"), col("BILL_TYPE_CD"), col("SERVICE_ADDRESS"),
                col("clientdiff"), col("parentdiff"))
              logger.info("ordercolsothers created")
              ordercolsothers.cache()
              ordercolsothers.show(false)
              val completeDf = Seq(dfCBunion, ordercolsothers)
              val dfcompleteunion = completeDf.reduce(_ union _).withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("UPDATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
              logger.info("dfcompleteunion created")
              dfcompleteunion.cache()
              if (dfcompleteunion.head(1).isEmpty) {
                logger.info("No data to process further !!!")
                throw new Exception("No data to process further")
              } else {
                val dfaddcols = dfcompleteunion.withColumnRenamed("ACCT_NBR", "ACCOUNT_NBR").withColumnRenamed("ASSOCIATION_CD", "HOUSE_ASSOC_CD")
                  .withColumnRenamed("PAR_NODE", "PARENT_NODE").withColumnRenamed("event_type", "EVENT_TYPE_CODE").withColumnRenamed("event_sub_type", "EVENT_SUB_TYPE_CODE")
                  .withColumnRenamed("PAR_NODE", "PARENT_NODE").withColumn("ACT_CUTOVER_START_DT", lit("")).withColumn("ACT_CUTOVER_END_DT", lit("")).withColumn("TICKET_ID", lit(""))
                  .withColumn("CPE_STATUS", lit("")).withColumn("CPE_STATUS_REASON", lit("")).withColumn("notif_id", lit(notif_id)).withColumn("EVENT_TYPE_CODE", lit("N+0")).drop("CC_NOTES")
                dfaddcols.cache()
                val orderonphoenix = dfcompleteunion.withColumnRenamed("ACCT_NBR", "CUST_ACCT_NMBR").withColumnRenamed("HOUSE_NBR", "HOUSE_NMBR").withColumnRenamed("PAR_NODE", "NODE")
                  .withColumnRenamed("TICKET_ID", "TICKETID").withColumnRenamed("BILL_TYPE_CD", "BILL_TYPE_CODE")
                  .withColumnRenamed("CUSTOMER_STATUS_CD", "CUSTOMER_STATUS_CODE").withColumnRenamed("PAR_BLD_STRT_DT", "SCHED_PAR_BUILD_START_DT")
                  .withColumnRenamed("CHLD_CUTOVER_STRT_DT", "SCHED_CHILD_CUTOVER_START_DT").withColumn("ACT_CUTOVER_START_DT", lit("")).withColumn("ACT_CUTOVER_END_DT", lit("")).withColumn("TICKETID", lit(""))
                  .withColumn("EVENT_MILESTONE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
                  .withColumn("CPE_STATUS", lit("")).withColumn("CPE_STATUS_REASON", lit("")).withColumn("EVENT_TYPE", lit("N+0"))
                  .select(lpad(col("CUST_ACCT_NMBR"), 9, "0").alias("CUST_ACCT_NMBR"), lpad(col("SITE_ID"), 3, "0").alias("SITE_ID"), $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE",
                    $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT",
                    $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CUSTOMER_KEY", $"CC_NOTES")
                //latestdf.select(lpad(col("CUST_ACCT_NMBR"),9,"0")).show(false)
                logger.info("orderonphoenix created")
                if (!orderonphoenix.head(1).isEmpty) {
                  logger.info("orderonphoenix ::::: not empty")
                  orderonphoenix.write.mode(SaveMode.Overwrite).parquet(processedstep1path + partition_date)
                  logger.info("orderonphoenix saved")
                } else {
                  logger.info("No data to process further !!!")
                  throw new Exception("No data to process further")
                }
                val dfnodeconvrpt = dfaddcols.select($"ACCOUNT_NBR", $"CUSTOMER_KEY", $"SITE_ID", $"HOUSE_NBR", $"HOUSE_ASSOC_CD", $"SIC_CD", $"SIC_DESC", $"CHILD_NODE", $"CHLD_BLD_STRT_DT", $"CHLD_CUTOVER_STRT_DT", $"PARENT_NODE", $"PAR_CUTOVER_STRT_DT", $"PAR_BLD_STRT_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKET_ID", $"CUSTOMER_STATUS_CD", $"CUSTOMER_CATEGORY_CD", $"CUSTOMER_CATEGORY_DESC", $"CUSTOMER_NM", $"DWELLING_TYPE_CD", $"DWELLING_TYPE_DESC",
                  $"SERVICE_ADDRESS", $"REASON_CODE", $"ACCT_FLG", $"EVENT_TYPE_CODE", $"EVENT_SUB_TYPE_CODE", $"CPE_STATUS", $"CPE_STATUS_REASON", $"BILL_TYPE_CD", $"CREATE_DT", $"UPDATE_DT")
                logger.info("dfnodeconvrpt created")
                if (!dfnodeconvrpt.head(1).isEmpty) {
                  dfnodeconvrpt.write.mode(SaveMode.Overwrite).option("delimiter", "\u0001").csv(processedrptpath)
                  logger.info("dfnodeconvrpt saved")
                } else {
                  throw new Exception("Step1 data processing failed")
                }

              }

            } else {
              logger.info("dfCBunion is not empty")
              val fetchotherrecs = ginsswattscusthouse.alias("a").join(dfCBunion.alias("b"), ginsswattscusthouse.col("SITE_ID") === dfCBunion.col("SITE_ID")
                && ginsswattscusthouse.col("ACCT_NBR") === dfCBunion.col("ACCT_NBR") && ginsswattscusthouse.col("HOUSE_NBR") === dfCBunion.col("HOUSE_NBR") && ginsswattscusthouse.col("CHILD_NODE") === dfCBunion.col("CHILD_NODE")
                && ginsswattscusthouse.col("PAR_NODE") === dfCBunion.col("PAR_NODE"), "left").filter(($"a.ACCT_NBR".isNotNull && $"b.ACCT_NBR".isNull) && ($"a.HOUSE_NBR".isNotNull && $"b.HOUSE_NBR".isNull)
                && ($"a.CHILD_NODE".isNotNull && $"b.CHILD_NODE".isNull) && ($"a.PAR_NODE".isNotNull && $"b.PAR_NODE".isNull)).select($"a.*").withColumn("event_type", lit("N+0")).withColumn("event_sub_type", lit("")).withColumn("CC_NOTES", lit(""))
              logger.info("fetchotherrecs created")
              val ordercolsothers = fetchotherrecs.select(col("event_sub_type"), col("event_type"), col("CC_NOTES"), col("ACCT_NBR"), col("SITE_ID"), col("ACCT_FLG"), col("CHILD_NODE"),
                col("CHLD_BLD_STRT_DT"), col("CHLD_CUTOVER_STRT_DT"), col("PAR_NODE"), col("PAR_CUTOVER_STRT_DT"), col("PAR_BLD_STRT_DT"), col("CUSTOMER_KEY"),
                col("SIC_DESC"), col("CUSTOMER_STATUS_CD"), col("CUSTOMER_CATEGORY_CD"), col("CUSTOMER_CATEGORY_DESC"), col("CUSTOMER_NM"), col("REASON_CODE"),
                col("HOUSE_NBR"), col("ASSOCIATION_CD"), col("SIC_CD"), col("DWELLING_TYPE_CD"), col("DWELLING_TYPE_DESC"), col("BILL_TYPE_CD"), col("SERVICE_ADDRESS"),
                col("clientdiff"), col("parentdiff"))
              logger.info("ordercolsothers created")
              val completeDf = Seq(dfCBunion, ordercolsothers)
              val dfcompleteunion = completeDf.reduce(_ union _).withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("UPDATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
              dfcompleteunion.cache
              if (dfcompleteunion.head(1).isEmpty) {
                logger.info("No data to process further !!!")
                throw new Exception("No data to process further")
              } else {
                val dfaddcols = dfcompleteunion.withColumnRenamed("ACCT_NBR", "ACCOUNT_NBR").withColumnRenamed("ASSOCIATION_CD", "HOUSE_ASSOC_CD")
                  .withColumnRenamed("PAR_NODE", "PARENT_NODE").withColumnRenamed("event_type", "EVENT_TYPE_CODE").withColumnRenamed("event_sub_type", "EVENT_SUB_TYPE_CODE")
                  .withColumnRenamed("PAR_NODE", "PARENT_NODE").withColumn("ACT_CUTOVER_START_DT", lit("")).withColumn("ACT_CUTOVER_END_DT", lit("")).withColumn("TICKET_ID", lit(""))
                  .withColumn("CPE_STATUS", lit("")).withColumn("CPE_STATUS_REASON", lit("")).withColumn("notif_id", lit(notif_id)).withColumn("EVENT_TYPE_CODE", lit("N+0")).drop("CC_NOTES")
                dfaddcols.cache
                val orderonphoenix = dfcompleteunion.withColumnRenamed("ACCT_NBR", "CUST_ACCT_NMBR").withColumnRenamed("HOUSE_NBR", "HOUSE_NMBR").withColumnRenamed("PAR_NODE", "NODE")
                  .withColumnRenamed("TICKET_ID", "TICKETID").withColumnRenamed("BILL_TYPE_CD", "BILL_TYPE_CODE")
                  .withColumnRenamed("CUSTOMER_STATUS_CD", "CUSTOMER_STATUS_CODE").withColumnRenamed("PAR_BLD_STRT_DT", "SCHED_PAR_BUILD_START_DT")
                  .withColumnRenamed("CHLD_CUTOVER_STRT_DT", "SCHED_CHILD_CUTOVER_START_DT").withColumn("ACT_CUTOVER_START_DT", lit("")).withColumn("ACT_CUTOVER_END_DT", lit("")).withColumn("TICKETID", lit(""))
                  .withColumn("EVENT_MILESTONE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
                  .withColumn("CPE_STATUS", lit("")).withColumn("CPE_STATUS_REASON", lit("")).withColumn("EVENT_TYPE", lit("N+0"))
                  .select(lpad(col("CUST_ACCT_NMBR"), 9, "0").alias("CUST_ACCT_NMBR"), lpad(col("SITE_ID"), 3, "0").alias("SITE_ID"), $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE",
                    $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT",
                    $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CUSTOMER_KEY", $"CC_NOTES")
                //latestdf.select(lpad(col("CUST_ACCT_NMBR"),9,"0")).show(false)
                logger.info("ordercolsothers created")
                if (!orderonphoenix.head(1).isEmpty) {
                  logger.debug("orderonphoenix ::::: " + orderonphoenix.count)
                  orderonphoenix.write.mode(SaveMode.Overwrite).parquet(processedstep1path + partition_date)
                  logger.info("orderonphoenix saved")
                } else {
                  logger.info("No data to process further !!!")
                  throw new Exception("No data to process further")
                }
                val dfnodeconvrpt = dfaddcols.select($"ACCOUNT_NBR", $"CUSTOMER_KEY", $"SITE_ID", $"HOUSE_NBR", $"HOUSE_ASSOC_CD", $"SIC_CD", $"SIC_DESC", $"CHILD_NODE", $"CHLD_BLD_STRT_DT", $"CHLD_CUTOVER_STRT_DT", $"PARENT_NODE", $"PAR_CUTOVER_STRT_DT", $"PAR_BLD_STRT_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKET_ID", $"CUSTOMER_STATUS_CD", $"CUSTOMER_CATEGORY_CD", $"CUSTOMER_CATEGORY_DESC", $"CUSTOMER_NM", $"DWELLING_TYPE_CD", $"DWELLING_TYPE_DESC",
                  $"SERVICE_ADDRESS", $"REASON_CODE", $"ACCT_FLG", $"EVENT_TYPE_CODE", $"EVENT_SUB_TYPE_CODE", $"CPE_STATUS", $"CPE_STATUS_REASON", $"BILL_TYPE_CD", $"CREATE_DT", $"UPDATE_DT")
                logger.info("dfnodeconvrpt created")
                if (!dfnodeconvrpt.head(1).isEmpty) {
                  dfnodeconvrpt.write.mode(SaveMode.Overwrite).option("delimiter", "\u0001").csv(processedrptpath)
                  logger.info("dfnodeconvrpt saved")
                } else {
                  throw new Exception("Step1 data processing failed")
                }
              }

            }

          } else {
            logger.info("dfunion is not null")
            dfunion.show(false)
            val fetchparentrecords = ginsswattscusthouse.alias("a").join(dfunion.alias("b"), ginsswattscusthouse.col("SITE_ID") === dfunion.col("SITE_ID") &&
              ginsswattscusthouse.col("ACCT_NBR") === dfunion.col("ACCT_NBR") && ginsswattscusthouse.col("HOUSE_NBR") === dfunion.col("HOUSE_NBR") &&
              ginsswattscusthouse.col("CHILD_NODE") === dfunion.col("CHILD_NODE") && ginsswattscusthouse.col("PAR_NODE") === dfunion.col("PAR_NODE"), "left")
              .filter(($"a.ACCT_NBR".isNotNull && $"b.ACCT_NBR".isNull) && ($"a.HOUSE_NBR".isNotNull && $"b.HOUSE_NBR".isNull) && ($"a.CHILD_NODE".isNotNull && $"b.CHILD_NODE".isNull)
                && ($"a.PAR_NODE".isNotNull && $"b.PAR_NODE".isNull)).select($"a.*")
            logger.info("fetchparentrecords created")
            val b2b14 = fetchparentrecords.filter((col("clientdiff") > 14 && col("BILL_TYPE_CD").!=("C")) || ((col("clientdiff") > (30) && col("BILL_TYPE_CD").===("C"))))
            logger.info("b2b14 created")
            b2b14.createOrReplaceTempView("b2b14t")
            spark.table("b2b14t").cache
            val b2b14data = spark.sql("select b.event_sub_type,b.event_type,b.icoms_cc_notes_txt,a.* from newmile b,b2b14t a where ((a.parentdiff>=b.build_threshold_low) and (a.parentdiff<=b.build_threshold_high))")
            b2b14data.show(false)            
            val combinedCBDf = Seq(dfunion, b2b14data)
            val dfCBunion = combinedCBDf.reduce(_ union _)
            dfCBunion.cache            
            if (dfCBunion.head(1).isEmpty) {
              logger.info("dfCBunion is null")
              val fetchotherrecs = ginsswattscusthouse.withColumn("event_type", lit("N+0")).withColumn("event_sub_type", lit("")).withColumn("CC_NOTES", lit(""))
              val ordercolsothers = fetchotherrecs.select(col("event_sub_type"), col("event_type"), col("CC_NOTES"), col("ACCT_NBR"), col("SITE_ID"), col("ACCT_FLG"), col("CHILD_NODE"),
                col("CHLD_BLD_STRT_DT"), col("CHLD_CUTOVER_STRT_DT"), col("PAR_NODE"), col("PAR_CUTOVER_STRT_DT"), col("PAR_BLD_STRT_DT"), col("CUSTOMER_KEY"),
                col("SIC_DESC"), col("CUSTOMER_STATUS_CD"), col("CUSTOMER_CATEGORY_CD"), col("CUSTOMER_CATEGORY_DESC"), col("CUSTOMER_NM"), col("REASON_CODE"),
                col("HOUSE_NBR"), col("ASSOCIATION_CD"), col("SIC_CD"), col("DWELLING_TYPE_CD"), col("DWELLING_TYPE_DESC"), col("BILL_TYPE_CD"), col("SERVICE_ADDRESS"),
                col("clientdiff"), col("parentdiff"))
              logger.info("ordercolsothers created")
              val completeDf = Seq(dfCBunion, ordercolsothers)
              val dfcompleteunion = completeDf.reduce(_ union _).withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("UPDATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
              dfcompleteunion.cache()
              if (dfcompleteunion.head(1).isEmpty) {
                logger.info("No data to process further !!!")
                throw new Exception("No data to process further")
              } else {
                val dfaddcols = dfcompleteunion.withColumnRenamed("ACCT_NBR", "ACCOUNT_NBR").withColumnRenamed("ASSOCIATION_CD", "HOUSE_ASSOC_CD")
                  .withColumnRenamed("PAR_NODE", "PARENT_NODE").withColumnRenamed("event_type", "EVENT_TYPE_CODE").withColumnRenamed("event_sub_type", "EVENT_SUB_TYPE_CODE")
                  .withColumnRenamed("PAR_NODE", "PARENT_NODE").withColumn("ACT_CUTOVER_START_DT", lit("")).withColumn("ACT_CUTOVER_END_DT", lit("")).withColumn("TICKET_ID", lit(""))
                  .withColumn("CPE_STATUS", lit("")).withColumn("CPE_STATUS_REASON", lit("")).withColumn("notif_id", lit(notif_id)).withColumn("EVENT_TYPE_CODE", lit("N+0")).drop("CC_NOTES")
                dfaddcols.cache()
                val orderonphoenix = dfcompleteunion.withColumnRenamed("ACCT_NBR", "CUST_ACCT_NMBR").withColumnRenamed("HOUSE_NBR", "HOUSE_NMBR").withColumnRenamed("PAR_NODE", "NODE")
                  .withColumnRenamed("TICKET_ID", "TICKETID").withColumnRenamed("BILL_TYPE_CD", "BILL_TYPE_CODE")
                  .withColumnRenamed("CUSTOMER_STATUS_CD", "CUSTOMER_STATUS_CODE").withColumnRenamed("PAR_BLD_STRT_DT", "SCHED_PAR_BUILD_START_DT")
                  .withColumnRenamed("CHLD_CUTOVER_STRT_DT", "SCHED_CHILD_CUTOVER_START_DT").withColumn("ACT_CUTOVER_START_DT", lit("")).withColumn("ACT_CUTOVER_END_DT", lit("")).withColumn("TICKETID", lit(""))
                  .withColumn("EVENT_MILESTONE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
                  .withColumn("CPE_STATUS", lit("")).withColumn("CPE_STATUS_REASON", lit("")).withColumn("EVENT_TYPE", lit("N+0"))
                  .select(lpad(col("CUST_ACCT_NMBR"), 9, "0").alias("CUST_ACCT_NMBR"), lpad(col("SITE_ID"), 3, "0").alias("SITE_ID"), $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE",
                    $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT",
                    $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CUSTOMER_KEY", $"CC_NOTES")
                //latestdf.select(lpad(col("CUST_ACCT_NMBR"),9,"0")).show(false)
                logger.info("orderonphoenix created")
                if (!orderonphoenix.head(1).isEmpty) {
                  logger.info("orderonphoenix ::::: not empty")
                  orderonphoenix.write.mode(SaveMode.Overwrite).parquet(processedstep1path + partition_date)
                  logger.info("orderonphoenix saved")
                } else {
                  logger.info("No data to process further !!!")
                  throw new Exception("No data to process further")
                }
                val dfnodeconvrpt = dfaddcols.select($"ACCOUNT_NBR", $"CUSTOMER_KEY", $"SITE_ID", $"HOUSE_NBR", $"HOUSE_ASSOC_CD", $"SIC_CD", $"SIC_DESC", $"CHILD_NODE", $"CHLD_BLD_STRT_DT", $"CHLD_CUTOVER_STRT_DT", $"PARENT_NODE", $"PAR_CUTOVER_STRT_DT", $"PAR_BLD_STRT_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKET_ID", $"CUSTOMER_STATUS_CD", $"CUSTOMER_CATEGORY_CD", $"CUSTOMER_CATEGORY_DESC", $"CUSTOMER_NM", $"DWELLING_TYPE_CD", $"DWELLING_TYPE_DESC",
                  $"SERVICE_ADDRESS", $"REASON_CODE", $"ACCT_FLG", $"EVENT_TYPE_CODE", $"EVENT_SUB_TYPE_CODE", $"CPE_STATUS", $"CPE_STATUS_REASON", $"BILL_TYPE_CD", $"CREATE_DT", $"UPDATE_DT")
                logger.info("dfnodeconvrpt created")
                if (!dfnodeconvrpt.head(1).isEmpty) {
                  dfnodeconvrpt.write.mode(SaveMode.Overwrite).option("delimiter", "\u0001").csv(processedrptpath)
                  logger.info("dfnodeconvrpt saved")
                } else {
                  throw new Exception("Step1 data processing failed")
                }

              }

            } else {
              ginsswattscusthouse.show(false)
              logger.info("dfCBunion is not null")
              dfCBunion.show(false)
              val fetchotherrecs = ginsswattscusthouse.alias("a").join(dfCBunion.alias("b"), ginsswattscusthouse.col("SITE_ID") === dfCBunion.col("SITE_ID")
                && ginsswattscusthouse.col("ACCT_NBR") === dfCBunion.col("ACCT_NBR") && ginsswattscusthouse.col("HOUSE_NBR") === dfCBunion.col("HOUSE_NBR") && ginsswattscusthouse.col("CHILD_NODE") === dfCBunion.col("CHILD_NODE")
                && ginsswattscusthouse.col("PAR_NODE") === dfCBunion.col("PAR_NODE"), "left").filter(($"a.ACCT_NBR".isNotNull && $"b.ACCT_NBR".isNull) && ($"a.HOUSE_NBR".isNotNull && $"b.HOUSE_NBR".isNull)
                && ($"a.CHILD_NODE".isNotNull && $"b.CHILD_NODE".isNull) && ($"a.PAR_NODE".isNotNull && $"b.PAR_NODE".isNull)).select($"a.*").withColumn("event_type", lit("N+0")).withColumn("event_sub_type", lit("")).withColumn("CC_NOTES", lit(""))
              logger.info("fetchotherrecs created")
              val ordercolsothers = fetchotherrecs.select(col("event_sub_type"), col("event_type"), col("CC_NOTES"), col("ACCT_NBR"), col("SITE_ID"), col("ACCT_FLG"), col("CHILD_NODE"),
                col("CHLD_BLD_STRT_DT"), col("CHLD_CUTOVER_STRT_DT"), col("PAR_NODE"), col("PAR_CUTOVER_STRT_DT"), col("PAR_BLD_STRT_DT"), col("CUSTOMER_KEY"),
                col("SIC_DESC"), col("CUSTOMER_STATUS_CD"), col("CUSTOMER_CATEGORY_CD"), col("CUSTOMER_CATEGORY_DESC"), col("CUSTOMER_NM"), col("REASON_CODE"),
                col("HOUSE_NBR"), col("ASSOCIATION_CD"), col("SIC_CD"), col("DWELLING_TYPE_CD"), col("DWELLING_TYPE_DESC"), col("BILL_TYPE_CD"), col("SERVICE_ADDRESS"),
                col("clientdiff"), col("parentdiff"))
              ordercolsothers.cache
              logger.info("ordercolsothers created")
              ordercolsothers.show(false)              
              val completeDf = Seq(dfCBunion, ordercolsothers)
              val dfcompleteunion = completeDf.reduce(_ union _).withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("UPDATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
              dfcompleteunion.cache
              if (dfcompleteunion.head(1).isEmpty) {
                logger.info("No data to process further !!!")
                throw new Exception("No data to process further")
              } else {
                logger.info("dfcompleteunion is not null")
                dfcompleteunion.show(false)
                val dfaddcols = dfcompleteunion.withColumnRenamed("ACCT_NBR", "ACCOUNT_NBR").withColumnRenamed("ASSOCIATION_CD", "HOUSE_ASSOC_CD")
                  .withColumnRenamed("PAR_NODE", "PARENT_NODE").withColumnRenamed("event_type", "EVENT_TYPE_CODE").withColumnRenamed("event_sub_type", "EVENT_SUB_TYPE_CODE")
                  .withColumnRenamed("PAR_NODE", "PARENT_NODE").withColumn("ACT_CUTOVER_START_DT", lit("")).withColumn("ACT_CUTOVER_END_DT", lit("")).withColumn("TICKET_ID", lit(""))
                  .withColumn("CPE_STATUS", lit("")).withColumn("CPE_STATUS_REASON", lit("")).withColumn("notif_id", lit(notif_id)).withColumn("EVENT_TYPE_CODE", lit("N+0")).drop("CC_NOTES")
                dfaddcols.cache
                val orderonphoenix = dfcompleteunion.withColumnRenamed("ACCT_NBR", "CUST_ACCT_NMBR").withColumnRenamed("HOUSE_NBR", "HOUSE_NMBR").withColumnRenamed("PAR_NODE", "NODE")
                  .withColumnRenamed("TICKET_ID", "TICKETID").withColumnRenamed("BILL_TYPE_CD", "BILL_TYPE_CODE")
                  .withColumnRenamed("CUSTOMER_STATUS_CD", "CUSTOMER_STATUS_CODE").withColumnRenamed("PAR_BLD_STRT_DT", "SCHED_PAR_BUILD_START_DT")
                  .withColumnRenamed("CHLD_CUTOVER_STRT_DT", "SCHED_CHILD_CUTOVER_START_DT").withColumn("ACT_CUTOVER_START_DT", lit("")).withColumn("ACT_CUTOVER_END_DT", lit("")).withColumn("TICKETID", lit(""))
                  .withColumn("EVENT_MILESTONE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
                  .withColumn("CPE_STATUS", lit("")).withColumn("CPE_STATUS_REASON", lit("")).withColumn("EVENT_TYPE", lit("N+0"))
                  .select(lpad(col("CUST_ACCT_NMBR"), 9, "0").alias("CUST_ACCT_NMBR"), lpad(col("SITE_ID"), 3, "0").alias("SITE_ID"), $"EVENT_TYPE", $"EVENT_SUB_TYPE", $"HOUSE_NMBR", $"NODE", $"CHILD_NODE", $"BILL_TYPE_CODE", $"CUSTOMER_STATUS_CODE",
                    $"CUSTOMER_CATEGORY_DESC", $"DWELLING_TYPE_DESC", $"SCHED_PAR_BUILD_START_DT", $"SCHED_CHILD_CUTOVER_START_DT", $"ACT_CUTOVER_START_DT",
                    $"ACT_CUTOVER_END_DT", $"TICKETID", $"CPE_STATUS", $"CPE_STATUS_REASON", $"CUSTOMER_KEY", $"CC_NOTES")
                //latestdf.select(lpad(col("CUST_ACCT_NMBR"),9,"0")).show(false)
                logger.info("ordercolsothers created")
                if (!orderonphoenix.head(1).isEmpty) {
                  logger.debug("orderonphoenix ::::: " + orderonphoenix.count)
                  orderonphoenix.write.mode(SaveMode.Overwrite).parquet(processedstep1path + partition_date)
                  logger.info("orderonphoenix saved")
                } else {
                  logger.info("No data to process further !!!")
                  throw new Exception("No data to process further")
                }
                val dfnodeconvrpt = dfaddcols.select($"ACCOUNT_NBR", $"CUSTOMER_KEY", $"SITE_ID", $"HOUSE_NBR", $"HOUSE_ASSOC_CD", $"SIC_CD", $"SIC_DESC", $"CHILD_NODE", $"CHLD_BLD_STRT_DT", $"CHLD_CUTOVER_STRT_DT", $"PARENT_NODE", $"PAR_CUTOVER_STRT_DT", $"PAR_BLD_STRT_DT", $"ACT_CUTOVER_START_DT", $"ACT_CUTOVER_END_DT", $"TICKET_ID", $"CUSTOMER_STATUS_CD", $"CUSTOMER_CATEGORY_CD", $"CUSTOMER_CATEGORY_DESC", $"CUSTOMER_NM", $"DWELLING_TYPE_CD", $"DWELLING_TYPE_DESC",
                  $"SERVICE_ADDRESS", $"REASON_CODE", $"ACCT_FLG", $"EVENT_TYPE_CODE", $"EVENT_SUB_TYPE_CODE", $"CPE_STATUS", $"CPE_STATUS_REASON", $"BILL_TYPE_CD", $"CREATE_DT", $"UPDATE_DT")
                logger.info("dfnodeconvrpt created")
                if (!dfnodeconvrpt.head(1).isEmpty) {
                  dfnodeconvrpt.write.mode(SaveMode.Overwrite).option("delimiter", "\u0001").csv(processedrptpath)
                  logger.info("dfnodeconvrpt saved")
                } else {
                  throw new Exception("Step1 data processing failed")
                }
              }

            }

          }
        }
        spark.catalog.clearCache()
      } else {
        logger.info("This program takes minimum 2 parameters.Please pass the property file and unixepoch time prepended with BATCH." + args.length)
        throw new Exception("This program takes minimum 2 parameters.Please pass the property file and unixepoch time prepended with BATCH.")
      }
    } catch {
      case noelem: NoSuchElementException =>
        logger.error("Dataframe is empty" + noelem.printStackTrace())
        throw noelem
      case ioe: IOException =>
        logger.error("IOException in MergeGinsWattsDimReal" + ioe.printStackTrace())
        throw ioe
      case confexp: ConfigException =>
        logger.error("ConfigException in MergeGinsWattsDimReal" + confexp.printStackTrace())
        throw confexp
    }
  }
}