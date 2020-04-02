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
import org.apache.spark.sql.types.TimestampType
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import java.text.ParseException

object WATTSDataProcessing extends App with LazyLogging {
  override def main(args: Array[String]): Unit = {
    logger.info("Inside the main program of WATTSDataProcessing: " + args.length)
    try {
      if (args.length > 2) { //&& !"".equals(args(1)) && args(1) != null) {
        logger.info(args(0) + " : " + args(1) + " : " + args(2))
        val cfgfile = args(0)
        val partition_date = args(1) //"2019-02-12"
        val previous_date = args(2) //""
        var accuVal = "" //args(3) //"11" //applicationConf.getString("") start from 0 for nw pass from cmd line later need to read the file and fetch
        val spark = SparkSession.builder().appName("Spark Reader for WATTS").getOrCreate()

        val sc = spark.sparkContext

        import spark.implicits._
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
        val parquettasklistpath = applicationConf.getString("spark.hdfs.parquettasklistpath.path")
        val parquetnodelistpath = applicationConf.getString("spark.hdfs.parquetnodelistpath.path")
        val parquetnodesplitpath = applicationConf.getString("spark.hdfs.parquetnodesplitpath.path")
        val parquetpath = applicationConf.getString("spark.hdfs.parquetpath.watts.processed.data.path")
        val keyfilecsvpath = applicationConf.getString("spark.hdfs.keyfilecsvpath.path")
        
        /**
         * timezone coversion
         */
        val convertUDF: UserDefinedFunction = udf((inputDate: String) => {
          if (!"".equals(inputDate) && inputDate != null) {
            try {
              val userDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
              userDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
              val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              val parsedDate = userDateFormat.format(format.parse(inputDate));
              parsedDate
            } catch {
              case e: ParseException =>
                logger.info("Invalid inputDate date format" + inputDate)
                ""
            }
          } else {
            ""
          }
        })

        val fs = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(keyfilecsvpath))
        var acc = sc.longAccumulator
        if (status.length > 0) {
          val csvDf = spark.read.format("csv").option("header", "true").load(keyfilecsvpath)
          val sss = csvDf.select("NODE_SPLIT_KEY").first()
          val r = sss.get(0)
          accuVal = r.toString()
          acc.add(accuVal.toLong)
        } else {
          accuVal = "0"
          acc.add(accuVal.toLong)
        }

        logger.info("acc:::::::" + acc.value)
        //Read the source parquet file
        val task_list = spark.read.parquet(parquettasklistpath + partition_date)
        val node_list = spark.read.parquet(parquetnodelistpath + partition_date)
        val node_split = spark.read.parquet(parquetnodesplitpath + partition_date)

        task_list.createOrReplaceTempView("tasktable")
        node_list.createOrReplaceTempView("nodetable")
        node_split.createOrReplaceTempView("nodesplittable")

        var df = spark.sql("select max(TIME_KEY) as TIME_KEY from nodesplittable").first()
        var max_time_key = df.get(0)
        var siteid_query = "CASE WHEN UPPER (MARKET) LIKE 'MACON'THEN 1 WHEN UPPER (MARKET) LIKE 'NEWORLEANS'THEN 126 WHEN UPPER (MARKET) LIKE 'OKLAHOMACITY'THEN 131 WHEN UPPER (MARKET) LIKE 'OMAHA'THEN 132 WHEN UPPER (MARKET) LIKE 'PENSACOLA'THEN 135 WHEN UPPER (MARKET) LIKE 'GLA'THEN 182 WHEN UPPER (MARKET) LIKE 'TULSA'THEN 186 WHEN  UPPER (MARKET) LIKE 'GAINESVILLE'THEN  214 WHEN  UPPER (MARKET) LIKE 'HAMPTONROADS'THEN  215 WHEN  UPPER (MARKET) LIKE 'CONNECTICUT'THEN  216 WHEN  UPPER (MARKET) LIKE 'RHODEISLAND'THEN  238 WHEN  UPPER (MARKET) LIKE 'ROANOKE'THEN  239 WHEN  UPPER (MARKET) LIKE 'ORANGECOUNTY'THEN  333 WHEN  UPPER (MARKET) LIKE 'PALOSVERDE'THEN  334 WHEN  UPPER (MARKET) LIKE 'SANTABARBARA'THEN  342 WHEN  UPPER (MARKET) LIKE 'PHOENIX'THEN  436 WHEN  UPPER (MARKET) LIKE 'LASVEGAS'THEN  476 WHEN  UPPER (MARKET) LIKE 'NORTHERNVA'THEN  477 WHEN  UPPER (MARKET) LIKE 'SANDIEGO'THEN  541 WHEN  UPPER (MARKET) LIKE 'KANSAS'THEN  580 WHEN  UPPER (MARKET) LIKE 'NWARKANSAS'THEN  580 WHEN  UPPER (MARKET) LIKE 'CLEVELAND'THEN  609 WHEN UPPER (MARKET) LIKE 'ORGCOUNTY' THEN 333 END"

        var dfdataset = spark.sql(s"""select p1.WORKORDER_ID,MARKET,$siteid_query as SITE_ID,case when upper(TASKCLASS) like '%FIBERBU%' or upper(TASKCLASS) like '%COAXBUIL%' then min(p2.TASKSTARTDATE) end as PARENT_BUILD_START_DT,case when upper(TASKCLASS) like '%CUT%' then min(p2.TASKSTARTDATE) end as PARENT_CUTOVER_DT,p1.TIME_KEY,p1.WORKFLOW_KEY,p1.ID,upper(ltrim(rtrim(p1.COXNODENUMBER))) as COXNODENUMBER from tasktable p2 inner join nodesplittable p1 on p1.TIME_KEY=p2.TIME_KEY and p1.WORKORDER_ID=p2.WORKORDER_ID and p1.WORKFLOW_KEY = p2.WORKFLOW_KEY where (upper(TASKCLASS) like '%CUT%' or upper(TASKCLASS) like '%FIBERBU%' or upper(TASKCLASS) like '%COAXBUIL%') and p1.COXNODENUMBER is not null and NVL(p1.ORDER_STATUS,'X') not in ('Resolved-Cancelled', 'Cancellation In Progress') and p1.TIME_KEY='$max_time_key' group by p1.WORKORDER_ID,MARKET,upper(TASKCLASS),p1.TIME_KEY,p1.WORKFLOW_KEY,p1.ID,upper(ltrim(rtrim(p1.COXNODENUMBER)))""")

        dfdataset.createOrReplaceTempView("datasettemp")

        var dfdataset1 = spark.sql("""select WORKORDER_ID,MARKET,SITE_ID,max(PARENT_BUILD_START_DT) as PARENT_BUILD_START_DT,max(PARENT_CUTOVER_DT) as PARENT_CUTOVER_DT,TIME_KEY,WORKFLOW_KEY,ID,COXNODENUMBER from datasettemp group by WORKORDER_ID, MARKET,SITE_ID,TIME_KEY,WORKFLOW_KEY,ID,COXNODENUMBER """)

        var dfdataset2 = spark.sql(s"""select  WORKORDER_ID,TIME_KEY,WORKFLOW_KEY,ltrim(rtrim(NAME)) as NAME,CUTOVERDATE,CONSTRUCTIONSTARTDATE,CONSTRUCTIONENDDATE,NODETYPE,COXNODENUMBER as NODE_COX_NBR from nodetable where NAME is not null and nvl(NODETYPE,'-')!= 'Legacy' and TIME_KEY='$max_time_key'""")

        dfdataset1.createOrReplaceTempView("dataset1")
        dfdataset2.createOrReplaceTempView("dataset2")

        var df3 = spark.sql("""select distinct p1.SITE_ID,p1.COXNODENUMBER as PAR_NODE,p1.PARENT_BUILD_START_DT as PAR_BLD_STRT_DT,p1.PARENT_CUTOVER_DT as PAR_CUTOVER_STRT_DT,p2.NAME as CHILD_NODE,p1.ID as ORDER_NAME,p2.CONSTRUCTIONSTARTDATE as CHLD_BLD_STRT_DT,p2.CUTOVERDATE as CHLD_CUTOVER_STRT_DT,p2.NODETYPE as ORDER_SUBTYPE,"Y" as CURR_FLG,date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS") as EFF_STRT_DT,"9999-12-31" as EFF_END_DT,date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS") as CREATE_DT,date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS") as LAST_UPD_DT from dataset1 p1 inner join dataset2 p2 on p1.TIME_KEY=p2.TIME_KEY and p1.WORKORDER_ID = p2.WORKORDER_ID and p1.WORKFLOW_KEY = p2.WORKFLOW_KEY where p1.SITE_ID is not null and p1.COXNODENUMBER is not null and p1.ID is not null""")

        //New date watts record
        var dfnew = df3.withColumn("CHLD_CUTOVER_STRT_TM", date_format(col("CHLD_CUTOVER_STRT_DT").cast(TimestampType), "yyyy-MM-dd HH:mm:ss").cast(TimestampType))

        //Type2 logic starts
        //TODO chk for the partition date if it already exists
        if (!"".equals(previous_date) && previous_date != null && previous_date.length() > 0) {
          logger.info("Compare History")
          val dfhist = spark.read.parquet(parquetpath + previous_date).filter(col("CURR_FLG") === "Y")

          var dfhistcol = dfhist.drop("CURR_FLG", "EFF_STRT_DT", "EFF_END_DT", "CREATE_DT", "LAST_UPD_DT").select("SITE_ID", "PAR_NODE", "PAR_BLD_STRT_DT", "PAR_CUTOVER_STRT_DT", "CHILD_NODE", "CHLD_BLD_STRT_DT", "CHLD_CUTOVER_STRT_DT", "CHLD_CUTOVER_STRT_TM", "ORDER_NAME", "ORDER_SUBTYPE")

          var dfcurrcol = dfnew.drop("CURR_FLG", "EFF_STRT_DT", "EFF_END_DT", "CREATE_DT", "LAST_UPD_DT").select("SITE_ID", "PAR_NODE", "PAR_BLD_STRT_DT", "PAR_CUTOVER_STRT_DT", "CHILD_NODE", "CHLD_BLD_STRT_DT", "CHLD_CUTOVER_STRT_DT", "CHLD_CUTOVER_STRT_TM", "ORDER_NAME", "ORDER_SUBTYPE")

          logger.info("histdropcoldf created")
          var updatedata = dfcurrcol.except(dfhistcol)
          if (updatedata.head(1).isEmpty) {
            logger.info("no change in the data")
            val histdatadf = spark.read.parquet(parquetpath + previous_date)
            logger.info("histdatadf" + histdatadf.count)
            val dfconv = histdatadf.withColumn("CHLD_CUTOVER_STRT_DT", convertUDF($"CHLD_CUTOVER_STRT_DT")).withColumn("PAR_CUTOVER_STRT_DT", convertUDF($"PAR_CUTOVER_STRT_DT"))
            dfconv.write.mode("overwrite").format("parquet").save(parquetpath + partition_date)
          } else {
            logger.info("updatedata created " + updatedata.count())
            updatedata.createOrReplaceTempView("nodesplitkey")
            val firstset = spark.sql(s"""select row_number() over (order by SITE_ID)+ ${acc.value} as NODE_SPLIT_KEY,* from nodesplitkey""")
            //firstset.coalesce(coalesceVal).write.mode(SaveMode.Append).parquet(parquetpath + partition_date)
            logger.info("firstset created saved " + firstset.count)

            var dfOlddata = dfhist.alias("a").join(firstset.alias("b"), firstset.col("SITE_ID") === dfhist.col("SITE_ID") && firstset.col("PAR_NODE") === dfhist.col("PAR_NODE") && firstset.col("CHILD_NODE") === dfhist.col("CHILD_NODE"), "leftsemi").select($"NODE_SPLIT_KEY", $"a.SITE_ID", $"a.PAR_NODE", $"a.PAR_BLD_STRT_DT", $"a.PAR_CUTOVER_STRT_DT", $"a.CHILD_NODE", $"a.ORDER_NAME", $"a.CHLD_BLD_STRT_DT", $"a.CHLD_CUTOVER_STRT_DT", $"a.ORDER_SUBTYPE", $"a.CURR_FLG", $"a.EFF_STRT_DT", $"a.EFF_END_DT", $"a.CREATE_DT", $"a.LAST_UPD_DT", $"a.CHLD_CUTOVER_STRT_TM")

            var dfsubOldData = dfhist.except(dfOlddata)

            var dfOldRecUp = dfOlddata.drop("CURR_FLG", "EFF_END_DT", "LAST_UPD_DT").withColumn("CURR_FLG", lit("N")).withColumn("EFF_END_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("LAST_UPD_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).select("NODE_SPLIT_KEY", "SITE_ID", "PAR_NODE", "PAR_BLD_STRT_DT", "PAR_CUTOVER_STRT_DT", "CHILD_NODE", "ORDER_NAME", "CHLD_BLD_STRT_DT", "CHLD_CUTOVER_STRT_DT", "ORDER_SUBTYPE", "CURR_FLG", "EFF_STRT_DT", "EFF_END_DT", "CREATE_DT", "LAST_UPD_DT", "CHLD_CUTOVER_STRT_TM")

            var dfnewSet = firstset.withColumn("CURR_FLG", lit("Y")).withColumn("EFF_STRT_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("EFF_END_DT", lit("12/31/9999")).withColumn("LAST_UPD_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).withColumn("CREATE_DT", date_format(current_timestamp(), "YYYY-MM-dd HH:MM:SS")).select("NODE_SPLIT_KEY", "SITE_ID", "PAR_NODE", "PAR_BLD_STRT_DT", "PAR_CUTOVER_STRT_DT", "CHILD_NODE", "ORDER_NAME", "CHLD_BLD_STRT_DT", "CHLD_CUTOVER_STRT_DT", "ORDER_SUBTYPE", "CURR_FLG", "EFF_STRT_DT", "EFF_END_DT", "CREATE_DT", "LAST_UPD_DT", "CHLD_CUTOVER_STRT_TM")
            var dfNewDateRecord = Seq(dfsubOldData, dfOldRecUp, dfnewSet)
            var dfUnion = dfNewDateRecord.reduce(_ union _)

            val surrogateK = dfUnion.select(col("NODE_SPLIT_KEY").cast(IntegerType)).select(max("NODE_SPLIT_KEY").alias("NODE_SPLIT_KEY"))
            surrogateK.select(max("NODE_SPLIT_KEY").alias("NODE_SPLIT_KEY")).write.option("header", "true").mode(SaveMode.Overwrite).csv(keyfilecsvpath)
            val dfconv = dfUnion.withColumn("CHLD_CUTOVER_STRT_DT", convertUDF($"CHLD_CUTOVER_STRT_DT")).withColumn("PAR_CUTOVER_STRT_DT", convertUDF($"PAR_CUTOVER_STRT_DT"))
            dfconv.write.mode("overwrite").format("parquet").save(parquetpath + partition_date)
            //logger.info("thirdset created saved " + thirdset.count)
            //TODO quality check
          }
        } else {
          logger.info("Load the data")
          dfnew.createOrReplaceTempView("nodesplitkey")
          val firstset = spark.sql(s"""select row_number() over (order by SITE_ID)+ ${acc.value} as NODE_SPLIT_KEY,* from nodesplitkey""")
          val dfconv = firstset.withColumn("CHLD_CUTOVER_STRT_DT", convertUDF($"CHLD_CUTOVER_STRT_DT")).withColumn("PAR_CUTOVER_STRT_DT", convertUDF($"PAR_CUTOVER_STRT_DT"))
          dfconv.write.mode("overwrite").format("parquet").save(parquetpath + partition_date)
          //dd.withColumn("NODE_ASSGN_KEY",$"NODE_ASSGN_KEY".cast(IntegerType)).select(max("NODE_ASSGN_KEY")).show
          firstset.withColumn("NODE_SPLIT_KEY", $"NODE_SPLIT_KEY".cast(IntegerType)).select(max("NODE_SPLIT_KEY").alias("NODE_SPLIT_KEY")).write.option("header", "true").mode(SaveMode.Overwrite).csv(keyfilecsvpath)
          logger.info("orderCols created saved")
          //TODO quality check

        }
        //TODO quality check

      } else {
        logger.info("This program takes minimum 2 parameters.Please pass the property file and the curr partition date and optional previous partition date." + args.length)
      }
    } catch {
      case noelem: NoSuchElementException => logger.error("Dataframe is empty" + noelem.printStackTrace())
      case ioe: IOException               => logger.error("IOException in GINSDataProcessing" + ioe.printStackTrace())
      case confexp: ConfigException       => logger.error("ConfigException in GINSDataProcessing" + confexp.printStackTrace())
    }
  }
}
		
		 


		  
		  
		  
		  
		
		
		




