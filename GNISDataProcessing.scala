package com.cox

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import java.io.IOException
import com.typesafe.config.ConfigException
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.LongType
import java.text.SimpleDateFormat
import java.text.ParseException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.max
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.datediff
import java.sql.Date
import org.apache.spark.sql.functions.count
/*
 * This program loads the gnis file and applies type2 logic and
 * prepares the data for further processing
 * It takes two parameters 
 * a) Config file b) previous partition to perform history comparision
 * It write the GNIS data back into hdfs location for further processing
 */
object GNISDataProcessing extends App with LazyLogging {
  override def main(args: Array[String]): Unit = {
    logger.info("Inside the main program of GNISDataProcessing: " + args.length)
    try {
      if (args.length == 2) {
        logger.info(args(0) + " : " + args(1) + " : ")
        val cfgfile = args(0)
        val partition_date = java.time.LocalDate.now
        
        val previous_date = args(1)
        var accuVal = ""
        val spark = SparkSession.builder.master("yarn-cluster").appName("Spark CSV Reader for GNIS").getOrCreate;
        logger.info("partition_date :" + partition_date)
        logger.info("previous_date :" + previous_date)
        val sc = spark.sparkContext
        logger.info("The GNIS data processing is started ")
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
        logger.info("Master key" + applicationConf.getString("spark.app.master"))
        val csvpath = applicationConf.getString("spark.hdfs.csvpath.gnis.data.path")
        val parquetpath = applicationConf.getString("spark.hdfs.parquet.gnis.processed.data.path")
        val keyfilecsvpath = applicationConf.getString("spark.hdfs.keyfilecsvpath.path")
        val qualitychkdata = applicationConf.getString("spark.hdfs.qualitycheckcsv.gnis.data")
        val csvschema = (new StructType).add("id", StringType).add("child_node", StringType).add("legacy_node", StringType)
          .add("disconnected", StringType).add("disconnect_date", StringType).add("site_id", StringType)
          .add("house_nbr", StringType).add("account_nbr", StringType).add("tag_number", StringType)
          .add("franchise_nbr", StringType).add("account_flg", StringType).add("status", StringType)
          .add("drop_date", StringType)
        val parquettemppath = applicationConf.getString("spark.hdfs.parquettemppath.gnis.data.path")
        logger.debug(csvpath + ":::" + parquetpath + ":::" + keyfilecsvpath + ":::" + qualitychkdata + ":::" + parquettemppath)
        
        /*
         * Converts csv data into parquet format
         */
        def convert() {
          logger.info("convert start:::")
          val tempdata = spark.read.schema(csvschema).option("header", "true").csv(csvpath + partition_date).filter(col("site_id").isNotNull && col("account_nbr").isNotNull)
          tempdata.write.mode(SaveMode.Overwrite).parquet(parquettemppath + partition_date)
          logger.debug("convert end:::" + tempdata.count)
          logger.info("convert end:::")
        }
        /*
         * Surrogate NODE_ASSGN_KEY key generation
         */
        val fs = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(keyfilecsvpath))
        var acc = sc.longAccumulator
        if (status.length > 0) {
          val csvDf = spark.read.format("csv").option("header", "true").load(keyfilecsvpath)
          val sss = csvDf.select("NODE_ASSGN_KEY").first()
          val r = sss.get(0)
          accuVal = r.toString()
          acc.add(accuVal.toLong)
        } else {
          accuVal = "0"
          acc.add(accuVal.toLong)
        }
        
        convert()
        
        logger.info("acc:::::::" + acc.value)
        val coalesceVal: Int = applicationConf.getString("spark.constant.coalesceVal").toInt
        //define all the types as string to perform data validation
        val validate_siteid = udf((site_id: String) => {
          if (!"".equals(site_id) && site_id != null && site_id.matches("[0-9]+")) "1"
          else "Invalid site_id as a character value"
        })
        val validate_housernbr = udf((house_nbr: String) => {
          if (!"".equals(house_nbr) && house_nbr != null && house_nbr.matches("[0-9]+")) "1"
          else "Invalid house_nbr as a character value"
        })
        val validate_accountnbr = udf((account_nbr: String) => {
          if (!"".equals(account_nbr) && account_nbr != null && account_nbr.matches("[0-9]+")) "1"
          else "Invalid account_nbr as a character value"
        })
        //validate the date formats
        val strDateformat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        def validate_disconnectdate(disconnect_date: String): Option[String] = {
          if (!"".equals(disconnect_date) && disconnect_date != null) {
            try {
              val dstr = strDateformat.parse(disconnect_date)
              return Some("1")
            } catch {
              case e: ParseException => return Some("Invalid disconnect_date date format")
            }
          } else {
            return Some("1")
          }
        }
        def validate_status(status: String): Option[String] = {
          if (!"".equals(status) && status != null) {
            try {
              val dstr = strDateformat.parse(status)
              return Some("1")
            } catch {
              case e: ParseException => return Some("Invalid status date format")
            }
          } else {
            return Some("1")
          }
        }
        def validate_dropdate(drop_date: String): Option[String] = {
          if (!"".equals(drop_date) && drop_date != null) {
            try {
              val dstr = strDateformat.parse(drop_date)
              return Some("1")
            } catch {
              case e: ParseException => return Some("Invalid drop_date date format")
            }
          } else {
            return Some("1")
          }
        }

        val udfdisconndate = udf(validate_disconnectdate _)
        val udfstatus = udf(validate_status _)
        val udfdropdate = udf(validate_dropdate _)

        val newdf = spark.read.schema(csvschema).parquet(parquettemppath + partition_date)
        logger.debug("newdf:::" + newdf.count())
        
        import spark.implicits._

        val df = newdf.withColumn("child_node", trim(newdf("child_node"))).withColumn("legacy_node", trim(newdf("legacy_node")))
          .withColumn("tag_number", trim(newdf("tag_number"))).withColumn("account_flg", trim(newdf("account_flg")))
          .withColumn("franchise_nbr", trim(newdf("franchise_nbr"))).withColumn("disconnected", trim(newdf("disconnected")))
          .withColumn("disconnect_date_status", newdf("disconnect_date"))
          .withColumn("validate_status", newdf("status")).withColumn("drop_date_status", newdf("drop_date"))
          .withColumn("siteid_status", validate_siteid(newdf("site_id"))).withColumn("accountnbr_status", validate_accountnbr(newdf("account_nbr")))
          .withColumn("house_nbrstatus", validate_housernbr(newdf("house_nbr")))
        logger.debug("df:::" + df.count)
        logger.info("df created")
        val validdata = df.filter(col("siteid_status") === "1" && col("accountnbr_status") === "1" && col("house_nbrstatus") === "1")
        val invalidatedata = df.filter(col("siteid_status").notEqual("1") || col("accountnbr_status").notEqual("1") || col("house_nbrstatus").notEqual("1"))
        /*if (validdata.head(1).isEmpty) {
          logger.info("Not data to process!!!")
          sc.stop()
        }*/
        logger.debug("validdata:::" + validdata.count)
        logger.info("validdata created")
        if (!"".equals(previous_date) && previous_date != null && previous_date.length() > 0) {
          logger.info("Compare History")
          val histdfcomplete = spark.read.parquet(parquetpath + previous_date)
          val histdf = spark.read.parquet(parquetpath + previous_date).filter(col("CURR_FLG") === "Y")
          logger.debug("histdf:::" + histdf.count())
          logger.info("histdf created")
          val histdropcoldf = histdf.drop("NODE_ASSGN_KEY", "CURR_FLG", "EFF_STRT_DT", "EFF_END_DT", "CREATE_DT", "LAST_UPD_DT")
            .select($"PAR_NODE", $"CHLD_NODE", $"ACCT_NBR", $"HOUSE_NBR", $"SITE_ID", $"DCNCT_DT", $"TAG_NBR", $"ACCT_FLG", $"FRNCHS_NBR", $"DISC_STAT", $"ASSGN_STAT", $"ASSGN_DROP_DT")
          logger.debug("histdropcoldf created" + histdropcoldf.count())
          logger.info("histdropcoldf created")
          val newdata = validdata.withColumnRenamed("legacy_node", "PAR_NODE").withColumnRenamed("child_node", "CHLD_NODE")
            .withColumnRenamed("account_nbr", "ACCT_NBR").withColumnRenamed("house_nbr", "HOUSE_NBR")
            .withColumnRenamed("site_id", "SITE_ID").withColumnRenamed("disconnect_date", "DCNCT_DT")
            .withColumnRenamed("tag_number", "TAG_NBR").withColumnRenamed("account_flg", "ACCT_FLG")
            .withColumnRenamed("franchise_nbr", "FRNCHS_NBR").withColumnRenamed("disconnected", "DISC_STAT")
            .withColumnRenamed("status", "ASSGN_STAT").withColumnRenamed("drop_date", "ASSGN_DROP_DT")
            .drop("id", "disconnect_date_status", "validate_status", "drop_date_status", "siteid_status", "accountnbr_status", "house_nbrstatus")
            .select($"PAR_NODE", $"CHLD_NODE", $"ACCT_NBR", $"HOUSE_NBR", $"SITE_ID", $"DCNCT_DT", $"TAG_NBR", $"ACCT_FLG", $"FRNCHS_NBR", $"DISC_STAT", $"ASSGN_STAT", $"ASSGN_DROP_DT")
          logger.debug("newdata created" + newdata.count)
          logger.info("newdata created")
          /*logger.info("histdfcomplete created" + histdfcomplete.printSchema())
          logger.info("newdata created" + newdata.printSchema())*/
          val delnewdata = histdfcomplete.alias("a").join(newdata.alias("b"), histdfcomplete.col("SITE_ID") === newdata.col("SITE_ID") && histdfcomplete.col("ACCT_NBR") === newdata.col("ACCT_NBR"), "left")
            .filter(($"a.ACCT_NBR".isNotNull && $"b.ACCT_NBR".isNull) && ($"a.SITE_ID".isNotNull && $"b.SITE_ID".isNull))
            .select($"a.NODE_ASSGN_KEY", $"a.ACCT_NBR", $"a.HOUSE_NBR", $"a.SITE_ID", $"a.PAR_NODE", $"a.CHLD_NODE", $"a.DCNCT_DT", $"a.TAG_NBR", $"a.ACCT_FLG", $"a.FRNCHS_NBR", $"a.DISC_STAT", $"a.ASSGN_STAT", $"a.ASSGN_DROP_DT", $"a.CURR_FLG", $"a.EFF_STRT_DT", $"a.EFF_END_DT", $"a.CREATE_DT", $"a.LAST_UPD_DT")
            .withColumn("CURR_FLG", lit("N")).withColumn("EFF_END_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
            .withColumn("LAST_UPD_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
            .select($"NODE_ASSGN_KEY", $"PAR_NODE", $"CHLD_NODE", $"ACCT_NBR", $"HOUSE_NBR", $"SITE_ID", $"DCNCT_DT", $"TAG_NBR", $"ACCT_FLG", $"FRNCHS_NBR", $"DISC_STAT", $"ASSGN_STAT", $"ASSGN_DROP_DT", $"CURR_FLG", $"EFF_STRT_DT", $"EFF_END_DT", $"CREATE_DT", $"LAST_UPD_DT")
          logger.debug("delnewdata created saved " + delnewdata.count)
          logger.info("delnewdata created")
          val updatedata = newdata.except(histdropcoldf)
          logger.info("updatedata created")
          logger.debug("updatedata" + updatedata.count())
          if (updatedata.head(1).isEmpty && delnewdata.head(1).isEmpty) {
            logger.info("no change in the data")            
            logger.debug("histdfcomplete" + histdfcomplete.count)
            histdfcomplete.coalesce(coalesceVal).write.mode(SaveMode.Overwrite).parquet(parquetpath + partition_date)           
          }else{
          logger.debug("updatedata created " + updatedata.count())
          val additionalfields = updatedata.withColumnRenamed("legacy_node", "PAR_NODE").withColumnRenamed("child_node", "CHLD_NODE")
            .withColumnRenamed("account_nbr", "ACCT_NBR").withColumnRenamed("house_nbr", "HOUSE_NBR").withColumnRenamed("site_id", "SITE_ID")
            .withColumnRenamed("disconnect_date", "DCNCT_DT").withColumnRenamed("tag_number", "TAG_NBR").withColumnRenamed("account_flg", "ACCT_FLG")
            .withColumnRenamed("franchise_nbr", "FRNCHS_NBR").withColumnRenamed("disconnected", "DISC_STAT").withColumnRenamed("status", "ASSGN_STAT")
            .withColumnRenamed("drop_date", "ASSGN_DROP_DT").drop("id").withColumn("CURR_FLG", lit("Y"))
            .withColumn("EFF_STRT_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("EFF_END_DT", lit("12/31/9999"))
            .withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
            .withColumn("LAST_UPD_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
          additionalfields.createOrReplaceTempView("nodeaasignkey")
          logger.debug("additionalfields created" + additionalfields.count)
          logger.info("additionalfields created")
          val firstset = spark.sql(s"""select row_number() over (order by ACCT_NBR)+ ${acc.value} as NODE_ASSGN_KEY,* from nodeaasignkey""")
          //firstset.coalesce(coalesceVal).write.mode(SaveMode.Append).parquet(parquetpath + partition_date)
          logger.debug("firstset created saved " + firstset.count)
          logger.info("firstset created")
          val stepone = newdata.alias("a").join(firstset.alias("b"), newdata.col("SITE_ID") === firstset.col("SITE_ID") && newdata.col("ACCT_NBR") === firstset.col("ACCT_NBR"), "left")
            .select($"a.ACCT_NBR", $"a.SITE_ID", $"b.ACCT_NBR".alias("accountnbr"), $"b.SITE_ID".alias("siteid")).filter(col("accountnbr").isNull && col("siteid").isNull).drop("accountnbr", "siteid")
          logger.info("stepone created")
          val secondset = histdf.alias("a").join(stepone.alias("b"), histdf.col("SITE_ID") === stepone.col("SITE_ID") && histdf.col("ACCT_NBR") === stepone.col("ACCT_NBR"), "inner")
            .select($"a.NODE_ASSGN_KEY", $"a.PAR_NODE", $"a.CHLD_NODE", $"a.ACCT_NBR", $"a.HOUSE_NBR", $"a.SITE_ID", $"a.DCNCT_DT", $"a.TAG_NBR", $"a.ACCT_FLG", $"a.FRNCHS_NBR", $"a.DISC_STAT", $"a.ASSGN_STAT", $"a.ASSGN_DROP_DT", $"a.CURR_FLG", $"a.EFF_STRT_DT", $"a.EFF_END_DT", $"a.CREATE_DT", $"a.LAST_UPD_DT")
          logger.info("secondset created")
          logger.debug("secondset created saved " + secondset.count)
          val thirdset = histdf.alias("a").join(firstset.alias("b"), histdf.col("SITE_ID") === firstset.col("SITE_ID") && histdf.col("ACCT_NBR") === firstset.col("ACCT_NBR"), "inner")
            .select($"a.NODE_ASSGN_KEY", $"a.ACCT_NBR", $"a.HOUSE_NBR", $"a.SITE_ID", $"a.PAR_NODE", $"a.CHLD_NODE", $"a.DCNCT_DT", $"a.TAG_NBR", $"a.ACCT_FLG", $"a.FRNCHS_NBR", $"a.DISC_STAT", $"a.ASSGN_STAT", $"a.ASSGN_DROP_DT", $"a.CURR_FLG", $"a.EFF_STRT_DT", $"a.EFF_END_DT", $"a.CREATE_DT", $"a.LAST_UPD_DT")
            .drop("CURR_FLG", "EFF_END_DT", "LAST_UPD_DT")
            .withColumn("CURR_FLG", lit("N")).withColumn("EFF_END_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
            .withColumn("LAST_UPD_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
            .select("NODE_ASSGN_KEY", "PAR_NODE", "CHLD_NODE", "ACCT_NBR", "HOUSE_NBR", "SITE_ID", "DCNCT_DT", "TAG_NBR", "ACCT_FLG", "FRNCHS_NBR", "DISC_STAT", "ASSGN_STAT", "ASSGN_DROP_DT", "CURR_FLG", "EFF_STRT_DT", "EFF_END_DT", "CREATE_DT", "LAST_UPD_DT")
          logger.debug("thirdset created saved " + thirdset.count)
          logger.info("thirdset created")          
          val combinedDf = Seq(firstset, secondset, thirdset, delnewdata)
          val dfunion = combinedDf.reduce(_ union _)
          dfunion.coalesce(coalesceVal).write.mode(SaveMode.Overwrite).parquet(parquetpath + partition_date)
          val surrogateK = dfunion.select(col("NODE_ASSGN_KEY").cast(IntegerType)).select(max("NODE_ASSGN_KEY").alias("NODE_ASSGN_KEY"))
          surrogateK.select(max("NODE_ASSGN_KEY").alias("NODE_ASSGN_KEY")).write.option("header", "true").mode(SaveMode.Overwrite).csv(keyfilecsvpath)
          logger.debug("surrogateK created saved " + thirdset.count)
          logger.info("surrogateK created")       
          
          val qualitytestdata = dfunion.groupBy("ACCT_NBR").agg(count("*").alias("cnt")).where($"cnt" > 1)
          if (qualitytestdata.count() > 0) {
            logger.info("customer is associated with multiple child nodes")
            val datachkd = qualitytestdata.alias("a").join(dfunion.alias("b"), qualitytestdata.col("ACCT_NBR") === dfunion.col("ACCT_NBR")).select(col("b.*"))
            datachkd.write.option("header", "true").mode(SaveMode.Overwrite).csv(qualitychkdata + partition_date)
            logger.info("qualitytestdata created")   
          }
        }
        } else {
          logger.info("Load the data")
          val additionalCols = validdata.withColumnRenamed("legacy_node", "PAR_NODE").withColumnRenamed("child_node", "CHLD_NODE")
            .withColumnRenamed("account_nbr", "ACCT_NBR").withColumnRenamed("house_nbr", "HOUSE_NBR").withColumnRenamed("site_id", "SITE_ID")
            .withColumnRenamed("disconnect_date", "DCNCT_DT").withColumnRenamed("tag_number", "TAG_NBR").withColumnRenamed("account_flg", "ACCT_FLG")
            .withColumnRenamed("franchise_nbr", "FRNCHS_NBR").withColumnRenamed("disconnected", "DISC_STAT").withColumnRenamed("status", "ASSGN_STAT")
            .withColumnRenamed("drop_date", "ASSGN_DROP_DT").drop("id").withColumn("CURR_FLG", lit("Y"))
            .withColumn("EFF_STRT_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("EFF_END_DT", lit("12/31/9999"))
            .withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS"))
            .withColumn("LAST_UPD_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).drop("id", "disconnect_date_status", "validate_status", "drop_date_status", "siteid_status", "accountnbr_status", "house_nbrstatus")
          logger.info("additionalCols created")
          additionalCols.createOrReplaceTempView("surrogatekey")
          val surrogatekey = spark.sql(s"""select row_number() over (order by ACCT_NBR)+ ${acc.value} as NODE_ASSGN_KEY,* from surrogatekey""")
          logger.info("surrogatekey created")
          val orderCols = surrogatekey.select(surrogatekey("NODE_ASSGN_KEY").cast(StringType).as("NODE_ASSGN_KEY"), surrogatekey("ACCT_NBR").cast(StringType).as("ACCT_NBR"), surrogatekey("HOUSE_NBR").cast(StringType).as("HOUSE_NBR"), surrogatekey("SITE_ID").cast(StringType).as("SITE_ID"), surrogatekey("PAR_NODE").cast(StringType).as("PAR_NODE"), surrogatekey("CHLD_NODE").cast(StringType).as("CHLD_NODE"), surrogatekey("DCNCT_DT").cast(StringType).as("DCNCT_DT"), surrogatekey("TAG_NBR").cast(StringType).as("TAG_NBR"), surrogatekey("ACCT_FLG").cast(StringType).as("ACCT_FLG"), surrogatekey("FRNCHS_NBR").cast(StringType).as("FRNCHS_NBR"), surrogatekey("DISC_STAT").cast(StringType).as("DISC_STAT"), surrogatekey("ASSGN_STAT").cast(StringType).as("ASSGN_STAT"), surrogatekey("ASSGN_DROP_DT").cast(StringType).as("ASSGN_DROP_DT"), surrogatekey("CURR_FLG").cast(StringType).as("CURR_FLG"), surrogatekey("EFF_STRT_DT").cast(StringType).as("EFF_STRT_DT"), surrogatekey("EFF_END_DT").cast(StringType).as("EFF_END_DT"), surrogatekey("CREATE_DT").cast(StringType).as("CREATE_DT"), surrogatekey("LAST_UPD_DT").cast(StringType).as("LAST_UPD_DT"))
          orderCols.coalesce(coalesceVal).write.mode(SaveMode.Overwrite).parquet(parquetpath + partition_date)
          
          val qualitydata = orderCols.groupBy("ACCT_NBR").agg(count("*").alias("cnt")).where($"cnt" > 1)
          if (qualitydata.count() > 0) {
            logger.info("customer is associated with multiple child nodes")
            val datachkd = qualitydata.alias("a").join(orderCols.alias("b"), qualitydata.col("ACCT_NBR") === orderCols.col("ACCT_NBR")).select(col("b.*"))
            datachkd.write.option("header", "true").mode(SaveMode.Overwrite).csv(qualitychkdata + partition_date)
            logger.info("qualitytestdata created")   
          }
          
          orderCols.withColumn("NODE_ASSGN_KEY", $"NODE_ASSGN_KEY".cast(IntegerType)).select(max("NODE_ASSGN_KEY").alias("NODE_ASSGN_KEY")).write.option("header", "true").mode(SaveMode.Overwrite).csv(keyfilecsvpath)
          logger.info("orderCols created saved")

        }

      } else {
        logger.info("This program takes minimum 2 parameters.Please pass the property file and optional previous partition date for the first time load." + args.length)
        throw new Exception("This program takes minimum 2 parameters.Please pass the property file and optional previous partition date for the first time load.")
      }
    } catch {
      case noelem: NoSuchElementException => logger.error("Dataframe is empty" + noelem.printStackTrace())
      case ioe: IOException               => logger.error("IOException in GNISDataProcessing" + ioe.printStackTrace())
      case confexp: ConfigException       => logger.error("ConfigException in GNISDataProcessing" + confexp.printStackTrace())
    }
  }
}
