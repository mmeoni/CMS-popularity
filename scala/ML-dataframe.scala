%dep
z.load("com.databricks:spark-csv_2.10:1.4.0")
z.load("com.databricks:spark-avro_2.10:2.0.1")

// Spark version
sc.version

val analytix = "hdfs://p01001532965510.cern.ch:9000"
val hadalytic = "hdfs://p01001532067275.cern.ch"

def getInputdir(s: String): String = s match {
 // case "eos"       => "/user/wdtmon/xrootd/cms/2015/{03,04,05,06,07}/*/" + s
    case "eos"       => hadalytic + "/user/wdtmon/xrootd/cms/2015/{03,04,05,06,07}/*/" + s
 // case "aaa"       => hadalytic + "/user/wdtmon/xrootd/cms/2015/*/" + s + "," + hadalytic + "/user/wdtmon/xrootd/cms/2016/{01,02,03}/*/" + s
 // case "aaa"       => "/user/wdtmon/xrootd/cms/2015/{03,04,05,06,07}/*/" + s
    case "aaa"       => "/user/wdtmon/xrootd/cms/2015/08/{01,02,03,04,05,06,07}/" + s
    case "crab"      => "/project/awg/cms/jm-data-popularity/avro-snappy/year=2015/*" // month=3/*"
    case "dbs3"      => "/project/awg/cms/dbs3verify/CMS_DBS3_PROD_GLOBAL/datasets/merged/*"
    case "dbs3-avro" => "/project/awg/cms/dbs3verify/datasets/part-m-00000.avro"
    case "phedex"    => "/project/awg/cms/phedex/catalog/csv/merged/"
    case "phedex-replicas" => "/project/awg/cms/phedex/block-replicas-snapshots/csv/time=2015*" // -08*
    case "cadi"      => "/user/mmeoni/spark-CADI/*"
  }

def getOutputdir(s: String): String = s match {
    case _ => "spark-ML/DATAFRAME_" + s.toUpperCase() + "_WEEK1"
  }
  
%sh
hadoop fs -ls spark-ML
hadoop fs -ls spark-DS/2015
hadoop fs -ls spark-DS/2016
//hadoop fs -ls spark-DS/POPULARITY
hadoop fs -ls /user/mmeoni/spark-CADI/*

// DBS 3
//val T_DBS = sqlc.read.format("com.databricks.spark.avro").load(getInputdir("dbs3-avro"))
val T_DBS = sqlc.read.format("com.databricks.spark.csv").load(getInputdir("dbs3")).toDF("id", "dataset_name") 
T_DBS.printSchema()
T_DBS.take(3).foreach(println)
T_DBS.count()

// CRAB
val T_CRAB = sqlc.read.format("com.databricks.spark.avro").load(getInputdir("crab"))
T_CRAB.printSchema()
T_CRAB.take(3).foreach(println)
T_CRAB.registerTempTable("T_CRAB")

val ML_dataframe_CRAB = sql(s"""
    SELECT 
        WEEKOFYEAR(from_unixtime(FinishedTimeStamp / 1000, 'yyyy-MM-dd')) as week,
        InputCollection AS dataset_name, 
        SPLIT(InputCollection,'/')[1] AS primarydataset,
        SPLIT(InputCollection,'/')[2] AS processeddataset,
        SPLIT(SPLIT(InputCollection,'/')[2],'-')[0] AS acquisitionera,
        SPLIT(SPLIT(InputCollection,'/')[2],'-')[1] AS processingversion,
        SPLIT(InputCollection,'/')[3] AS datatier,
        TargetCE AS client_domain,
        "cern.ch" AS server_domain,
        SiteName AS server_site,
        ProtocolUsed AS user_protocol, 
        GridName AS username,
        0 AS dataset_size,
        COUNT(InputCollection) AS numaccesses,
        SUM(WrapCPU) AS proctime,
        SUM(WrapWC) AS readbytes
    FROM 
        T_CRAB
    GROUP BY
        WEEKOFYEAR(from_unixtime(FinishedTimeStamp / 1000, 'yyyy-MM-dd')),
        InputCollection, 
        SPLIT(InputCollection,'/')[1],
        SPLIT(InputCollection,'/')[2],
        SPLIT(SPLIT(InputCollection,'/')[2],'-')[0],
        SPLIT(SPLIT(InputCollection,'/')[2],'-')[1],
        SPLIT(InputCollection,'/')[3],
         TargetCE,
        "cern.ch",
        SiteName,
        ProtocolUsed, 
        GridName,
        0
""")

ML_dataframe_CRAB.take(5).foreach(println)
ML_dataframe_CRAB.count()

val T_XRD_HDFS = sqlc.read.format("com.databricks.spark.csv").load("spark-DS/2015/T_XRD_HDFS_AAA").toDF("_corrupt_record","app_info","client_domain","client_host","end_time","file_lfn","file_size","read_average","read_bytes","read_bytes_at_close","read_max","read_min","read_operations","read_sigma","read_single_average","read_single_bytes","read_single_max","read_single_min","read_single_operations","read_single_sigma","read_vector_average","read_vector_bytes","read_vector_count_average","read_vector_count_max","read_vector_count_min","read_vector_count_sigma","read_vector_max","read_vector_min","read_vector_operations","read_vector_sigma","server_domain","server_host","server_site","server_username","start_time","unique_id","user_dn","user_fqan","user_protocol","user_role","user_vo","write_average","write_bytes","write_bytes_at_close","write_max","write_min","write_operations","write_sigma")
//T_XRD_HDFS.count()
//val T_XRD_RAW_FILE = sqlc.read.format("com.databricks.spark.csv").load("spark-DS/2015/T_XRD_RAW_FILE_AAA").toDF("TDay", "ots", "cts", "file_lfn", "client_host", "server_username", "client_domain", "server_domain", "server_site", "user_protocol", "proctime", "readbytes")

// XROOTD
val T_XRD_HDFS = sqlc.read.json(getInputdir("aaa"))
//T_XRD_HDFS.printSchema()
//T_XRD_HDFS.take(3).foreach(println)
T_XRD_HDFS.registerTempTable("T_XRD_HDFS")
T_XRD_HDFS.count()

val T_XRD_RAW_FILE = sql("select from_unixtime(end_time, 'yyyy/MM/dd') as TDay, start_time as ots, end_time as cts, file_lfn, client_domain, client_host, if(server_username = '', 'unknown', server_username) as server_username, server_domain, server_host, server_site, 'xrootd' AS user_protocol, (end_time - start_time) as proctime, read_bytes_at_close as readbytes FROM T_XRD_HDFS WHERE (end_time - start_time) > 0 AND read_bytes_at_close > 0 AND `_corrupt_record` IS NULL")

//T_XRD_RAW_FILE.take(3).foreach(println)
T_XRD_RAW_FILE.registerTempTable("T_XRD_RAW_FILE")
//T_XRD_RAW_FILE.count()

// Phedex File Catalog
val T_XRD_LFC = sqlc.read.format("com.databricks.spark.csv").load(getInputdir("phedex")).toDF("dataset_name", "dataset_id", "dataset_is_open", "dataset_time_create", "block_name", "block_id", "block_time_create", "block_is_open", "file_lfn", "file_id", "filesize", "usernameXX", "checksum", "file_time_create")
//T_XRD_LFC.take(3).foreach(println)
T_XRD_LFC.registerTempTable("T_XRD_LFC")
//T_XRD_LFC.count()

//T_XRD_HDFS.write.format("com.databricks.spark.csv").option("header", "false").save("spark-DS/2016/T_XRD_HDFS_AAA")
T_XRD_RAW_FILE.write.format("com.databricks.spark.csv").option("header", "false").save("spark-DS/2016/T_XRD_RAW_FILE_AAA")

// Phedex Block Replicas (./block-replicas-snapshots/csv/time=2015-08*)
val T_BLOCK_REPLICAS_SNAPSHOTS = sqlc.read.format("com.databricks.spark.csv").load(getInputdir("phedex-replicas")).toDF("now", "dataset_name", "dataset_id", "dataset_is_open", "dataset_time_create", "dataset_time_update", "block_name", "block_id", "block_files", "block_bytes", "block_is_open", "block_time_create", "block_time_update", "node_name", "node_id", "br.is_active", "br.src_files", "br.src_bytes", "br.dest_files", "br.dest_bytes", "br.node_files", "br_node_bytes", "br.xfer_files", "br.xfer_bytes", "br.is_custodial", "br.user_group", "replica_time_create", "replica_time_update")
//T_BLOCK_REPLICAS_SNAPSHOTS.printSchema()
//T_BLOCK_REPLICAS_SNAPSHOTS.take(3).foreach(println)
//T_BLOCK_REPLICAS_SNAPSHOTS.count()
T_BLOCK_REPLICAS_SNAPSHOTS.registerTempTable("T_BLOCK_REPLICAS_SNAPSHOTS")

// Select only replicas from disk accessible by users
val TEMP_sizeofblockreplicas = sql(s"""
    SELECT sum(br_node_bytes) as sizeofblockreplicas, dataset_name 
    FROM T_BLOCK_REPLICAS_SNAPSHOTS
    WHERE node_name not like '%MSS' and node_name not like '%Buffer' and node_name not like '%Export'
    GROUP BY dataset_name
""")
TEMP_sizeofblockreplicas.registerTempTable("TEMP_sizeofblockreplicas")

val TEMP_sizeofblocks = sql(s"""
    SELECT sum(temp.block_bytes) as sizeofblocks, temp.dataset_name
    FROM (SELECT DISTINCT block_bytes, block_id, dataset_name FROM T_BLOCK_REPLICAS_SNAPSHOTS) AS temp
    GROUP BY temp.dataset_name
""")
TEMP_sizeofblocks.registerTempTable("TEMP_sizeofblocks")

val REPLICAS_dataframe = sql(s"""
    SELECT 
        FLOOR(a.sizeofblockreplicas/b.sizeofblocks) AS replicas, 
        a.dataset_name 
    FROM
        TEMP_sizeofblockreplicas a, TEMP_sizeofblocks b
    WHERE 
        a.dataset_name = b.dataset_name AND FLOOR(a.sizeofblockreplicas/b.sizeofblocks) > 1
""")
REPLICAS_dataframe.registerTempTable("REPLICAS_dataframe")
REPLICAS_dataframe.take(10).foreach(println)
//REPLICAS_dataframe.count()

val ML_plots = sql(s"""
   SELECT 
        CEILING(LN(replicas)) AS replicas,
        COUNT(*) as occurrences
   FROM
        REPLICAS_dataframe
   GROUP BY 
        CEILING(LN(replicas))
""")

ML_plots.registerTempTable("ML_plots")
z.show(sql("select * from ML_plots order by occurrences"))

// CADI (./spark-CADI/CADI.json)
val T_CADI = sqlc.read.format("json").load(getInputdir("cadi"))//.toDF("Conference","code","physWG","name","sources","creatorName","targetDatePreApp","targetDatePub","contact","samples","description")
T_CADI.printSchema()
T_CADI.take(3).foreach(println)
T_CADI.count()
T_CADI.registerTempTable("T_CADI")

T_CADI.filter("Conference = 'ICHEP2014'").collect().foreach(println)

z.show(sql(s"""
    SELECT DISTINCT conference, physWG, samples FROM T_CADI
"""))

val ML_dataframe_xrootd = sql(s"""
    SELECT  
        WEEKOFYEAR(regexp_replace(raw.Tday, '/', '-')) AS week, 
        lfc.dataset_name,
        SPLIT(lfc.dataset_name,'/')[1] AS primarydataset,
        SPLIT(lfc.dataset_name,'/')[2] AS processeddataset,
        SPLIT(SPLIT(lfc.dataset_name,'/')[2],'-')[0] AS acquisitionera,
        SPLIT(SPLIT(lfc.dataset_name,'/')[2],'-')[1] AS processingversion,
        SPLIT(lfc.dataset_name,'/')[3] AS datatier,
        raw.client_domain, 
        lfc.usernameXX AS username,
        raw.server_domain, 
        raw.server_site, 
        raw.user_protocol,
        SUM(lfc.filesize) AS dataset_size,
        COUNT(raw.client_host) AS naccesses,
        SUM(raw.proctime) AS proctime,
        SUM(raw.readbytes) AS readbytes 
    FROM 
        T_XRD_RAW_FILE raw,
        T_XRD_LFC lfc 
    WHERE 
        raw.file_lfn = lfc.file_lfn 
    GROUP BY
        WEEKOFYEAR(regexp_replace(raw.Tday, '/', '-')), 
        lfc.dataset_name,
        split(dataset_name,'/')[2],
        split(dataset_name,'/')[3],
        SPLIT(SPLIT(lfc.dataset_name,'/')[2],'-')[0],
        SPLIT(SPLIT(lfc.dataset_name,'/')[2],'-')[1],
        SPLIT(lfc.dataset_name,'/')[3],
        raw.client_domain,
        lfc.usernameXX,
        raw.server_domain, 
        raw.server_site, 
        raw.user_protocol
""")

ML_dataframe_xrootd.count()
ML_dataframe_xrootd.registerTempTable("ML_dataframe_xrootd")

z.show(sql("select * from ML_dataframe_xrootd LIMIT 3"))

// Write ML_dataframe(s) to HDFS
//ML_dataframe_xrootd.write.format("com.databricks.spark.csv").option("header", "false").save(getOutputdir("eos"))
ML_dataframe_xrootd.write.format("com.databricks.spark.csv").option("header", "false").save(getOutputdir("aaa"))
//ML_dataframe_CRAB.write.format("com.databricks.spark.csv").option("header", "false").save(getOutputdir("crab"))

// Load all ML_dataframe(s) and prepare plots for thresholds' understanding
val ML_dataframes = sqlc.read.format("com.databricks.spark.csv").load(getOutputdir("aaa")).toDF("week", "dataset_name", "primarydataset", "processeddataset", "acquisitionera", "processingversion", "datatier", "client_domain", "server_domain", "server_site", "user_protocol", "username", "dataset_size", "numaccesses", "proctime", "readbytes")
ML_dataframes.registerTempTable("ML_dataframes")
//z.show(sql("select * from ML_dataframes LIMIT 3"))
//ML_dataframes.count()

// Produce popularity metrics' plots
// (readbytes, naccesses, proctime) 
val ML_plots = sql(s"""
   SELECT 
        CEILING(LN(dataset_size)) AS dataset_size,
        COUNT(*) as occurrences
   FROM
        ML_dataframes
   GROUP BY 
        CEILING(LN(dataset_size))
""")
ML_plots.registerTempTable("ML_plots")

// Produce popularity metrics' plots
// (nusers)
val ML_plots_pre = sql(s"""
   SELECT 
        dataset_name, username,
        COUNT(*) as nusers
   FROM
        ML_dataframes
   GROUP BY 
        dataset_name, username
""")
ML_plots_pre.take(3).foreach(println)

ML_plots_pre.registerTempTable("ML_plots_pre")
val ML_plots = sql(s"""
   SELECT 
        CEILING(LN(nusers)) AS nusers,
        COUNT(*) as occurrences
   FROM
        ML_plots_pre
   GROUP BY 
        CEILING(LN(nusers))
""")

ML_plots.registerTempTable("ML_plots")
z.show(sql("select * from ML_plots order by occurrences"))
