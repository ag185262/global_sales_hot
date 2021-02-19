package com.ncr.edl.apollo.common

import java.sql.Timestamp
import java.util.Date
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.io.Source
import java.io.File
import java.util.regex.Pattern
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext

class ExecutorDao(activityCountRequired: Boolean, appName: String) {

  val WITH = "WITH "
  val INSERT_KEYWORD_MATCHED = "(?i)\\bINSERT\\b"
  val CREATE_KEYWORD_MATCHED = "\\bCREATE\\b"
  val TABLE_KEYWORD_MATCHER = "(?i)\\bTABLE\\b"
  val OVERWRITE_KEYWORD_MATCHER = "(?i)\\bOVERWRITE\\b"
  val INTO_KEYWORD_MATCHER = "(?i)\\bINTO\\b"
  val IF_NOT_EXISTS_MATCHER = "IF\\s+NOT\\s+EXISTS"
  val INSERT = "INSERT "
  val CREATE = "CREATE "

  var sparkSession = SparkSession.builder.appName(appName).enableHiveSupport().getOrCreate()
  sparkSession.sparkContext.setLogLevel("ERROR")

  var currentGroup = appName

  def preProcess() {
  }

  def setCurrentGroup(group: String) {
    currentGroup = group
  }

  def setLocalProperty(key: String, value: String) {
    sparkSession.sparkContext.setLocalProperty(key, value)
  }

  def setCallSiteLongProperty(description: String) {
    setLocalProperty("callSite.long", description)
  }

  def setCallSiteShortProperty(description: String) {
    setLocalProperty("callSite.short", "sql in group: " + description)
  }

  def clearSparkSessionCache() {
    try {
      sparkSession.catalog.clearCache()
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def executeQueryAndSaveToDataframe(query: String): DataFrame = {
    setCallSiteShortProperty(currentGroup)
    setCallSiteLongProperty(query)
    return executeWithRetry(query)
  }

  def executeWithRetry(query: String): DataFrame = {
    var isSuccess = false
    var df: DataFrame = null
    try {
      df = sparkSession.sql(query)
      df.show()
      isSuccess = true
    } catch {
      case e: Exception           => e.printStackTrace()
      case e: java.lang.Exception => e.printStackTrace()
    } finally {
      if (!isSuccess) {
        println("First attempt failed. Retrying...")
        df = sparkSession.sql(query)
      }
    }
    return df
  }

  def executeQuery(query: String) {

    val impactedRowCountRequired = activityCountRequired && (isInsertQuery(query) || isCreateQuery(query))
    var preExecutionRowCount = 0L
    var tableLocation = ""
    var tableType = ""
    var impactedTable = ""
    println("Running Query: " + query)
    var query_execution_start_time = new Timestamp(new Date().getTime())
    println("Query execution start time: " + query_execution_start_time)

    if (impactedRowCountRequired) {
      println("Impacted row count for query will be required.")
      impactedTable = this.getImpactedTableFromQuery(query)
      println("Impacted table in query is %s".format(impactedTable))
      var tableProperties = this.getTableProperties(impactedTable)
      tableLocation = tableProperties._1
      tableType = tableProperties._2
      println("Location for table `%s` is `%s`".format(impactedTable, tableLocation))
      preExecutionRowCount = this.getRowCountForTable(tableLocation, tableType)
      println("Pre execution row count for impacted table `%s` is %s".format(impactedTable, preExecutionRowCount))
    }
    println("Query execution start timestamp: " + System.currentTimeMillis())
    setCallSiteShortProperty(currentGroup)
    setCallSiteLongProperty(query)
    executeWithRetry(query)
    println("Query execution end timestamp: " + System.currentTimeMillis())
    var query_execution_end_time = new Timestamp(new Date().getTime())
    var query_execution_time = this.getExecutionTime(query_execution_start_time, query_execution_end_time)
    println("Query execution end time: " + query_execution_end_time)
    println("Total execution time: " + query_execution_time)
    if (impactedRowCountRequired) {
      val postExecutionRowCount = this.getRowCountForTable(tableLocation, tableType)
      println("Post execution row count for impacted table `%s` is %s".format(impactedTable, postExecutionRowCount))
      val impactedRows = (postExecutionRowCount - preExecutionRowCount).abs
      println("Impacted row count: " + impactedRows)
    }
  }

  def isInsertQuery(text: String): Boolean = {
    var query = text.toUpperCase().trim()
    if (query.startsWith(WITH)) {
      query = query.replaceAll(INSERT_KEYWORD_MATCHED, INSERT)
      return Pattern.compile(INSERT_KEYWORD_MATCHED).matcher(query).find()
    }
    return query.trim().toUpperCase().startsWith(INSERT)
  }

  def isCreateQuery(text: String): Boolean = {
    var query = text.toUpperCase().trim()
    if (query.startsWith(WITH)) {
      query = query.replaceAll(CREATE_KEYWORD_MATCHED, CREATE)
      return Pattern.compile(CREATE_KEYWORD_MATCHED).matcher(query).find()
    }
    return query.trim().toUpperCase().startsWith(CREATE)
  }

  def getImpactedTableFromQuery(text: String): String = {
    if (isInsertQuery(text)) {
      return getImpactedTableFromInsertQuery(text)
    } else if (isCreateQuery(text)) {
      return getImpactedTableFromCreateQuery(text)
    } else {
      return ""
    }
  }

  def getImpactedTableFromInsertQuery(text: String): String = {
    var query = text.toUpperCase().trim()
    if (query.startsWith(WITH)) {
      val queryWithCTE = query.replaceAll(INSERT_KEYWORD_MATCHED, INSERT)
      val formatedQueryWithCTE = queryWithCTE.substring(queryWithCTE.indexOf(INSERT), queryWithCTE.length())
      query = formatedQueryWithCTE
    }
    val queryWithoutInsertKeyword = query.replaceFirst(INSERT_KEYWORD_MATCHED, " ")
    val queryWithoutIntoKeyword = queryWithoutInsertKeyword.replaceFirst(INTO_KEYWORD_MATCHER, " ")
    val queryWithoutOverwriteKeyword = queryWithoutIntoKeyword.replaceFirst(OVERWRITE_KEYWORD_MATCHER, " ")
    val queryWithoutTableKeyword = queryWithoutOverwriteKeyword.replaceFirst(TABLE_KEYWORD_MATCHER, " ").trim()
    return queryWithoutTableKeyword.split(" ")(0).trim()
  }

  def getImpactedTableFromCreateQuery(text: String): String = {
    var query = text.toUpperCase().trim()
    if (query.startsWith(WITH)) {
      val queryWithCTE = query.replaceAll(CREATE_KEYWORD_MATCHED, INSERT)
      val formatedQueryWithCTE = queryWithCTE.substring(queryWithCTE.indexOf(CREATE), queryWithCTE.length())
      query = formatedQueryWithCTE
    }
    val queryWithoutInsertKeyword = query.replaceFirst(CREATE_KEYWORD_MATCHED, " ")
    val queryWithoutIntoKeyword = queryWithoutInsertKeyword.replaceFirst(TABLE_KEYWORD_MATCHER, " ")
    val queryWithoutOverwriteKeyword = queryWithoutIntoKeyword.replaceFirst(IF_NOT_EXISTS_MATCHER, " ").trim()
    return queryWithoutOverwriteKeyword.split(" ")(0).trim()
  }

  def getTableProperties(table: String): (String, String) = {
    if (table == null || table.isEmpty()) {
      return ("", "")
    }
    var tableLocation = ""
    var tableType = ""
    try {
      val tableDescription = sparkSession.sql("desc formatted " + table)
      val tableLocations = tableDescription.filter("trim(upper(col_name)) = trim(upper('LOCATION'))").select("data_type")
      val tableInputFormat = tableDescription.filter("trim(upper(col_name)) = trim(upper('INPUTFORMAT'))").select("data_type")

      if (tableLocations.count() > 0) {
        tableLocation = tableLocations.first().get(0).toString()
      }
      if (tableInputFormat.count() > 0) {
        var tableInputFormatVal = tableInputFormat.first().get(0).toString()
        if (tableInputFormatVal.contains("OrcInputFormat")) {
          tableType = "orc"
        } else if (tableInputFormatVal.contains("TextInputFormat")) {
          tableType = "text"
        } else {
          tableType = "parquet"
        }
      }
    } catch {
      case e: Exception => {
        println("Error while getting properties for table `%s`: %s".format(table, e))
      }
    }
    return (tableLocation, tableType)
  }

  def getRowCountForTable(tableLocation: String, tableType: String): Long = {
    if (tableLocation.isEmpty() || tableType.isEmpty()) {
      return 0L
    }
    var rowCount = 0L
    try {
      if (tableType.equalsIgnoreCase("orc")) {
        rowCount = sparkSession.read.orc(tableLocation).count()
      } else if (tableType.equalsIgnoreCase("text")) {
        rowCount = sparkSession.read.csv(tableLocation).count()
      } else {
        rowCount = sparkSession.read.parquet(tableLocation).count()
      }
    } catch {
      case e: Exception => {
        println("Error while collecting row count for `%s`: %s".format(tableLocation, e))
      }
    }
    return rowCount
  }

  def getExecutionTime(startTime: Timestamp, endTime: Timestamp): String = {
    var milliseconds = endTime.getTime() - startTime.getTime()
    var seconds = milliseconds.toInt / 1000

    var hours = seconds / 3600
    var minutes = (seconds % 3600) / 60
    seconds = (seconds % 3600) % 60
    return "%s Hours %s Minutes %s Seconds"
      .format(
        hours.toString(),
        minutes.toString(),
        seconds.toString())
  }

  def loadProperties(propertyFilePath: String): scala.collection.mutable.Map[String, String] = {

    var mode: String = sparkSession.sparkContext.getConf.get("spark.submit.deployMode")

    if ((mode.equalsIgnoreCase("client") || mode.equalsIgnoreCase("local")) && propertyFilePath.trim().startsWith("/")) {
      var dbParams = scala.collection.mutable.Map[String, String]()
      var propertyFile = new File(propertyFilePath)
      if (!propertyFile.exists) {
        throw new Exception("Provided property file does not exists.")
      }
      if (!propertyFile.isFile()) {
        throw new Exception("Invalid property File.")
      }
      for (line <- Source.fromFile(propertyFilePath).getLines) {
        if (line.contains("=")) {
          var split = line.split("=")
          dbParams.put(split(0).trim(), split(1).trim())
        }
      }
      return dbParams
    } else {
      return loadPropertiesFromHDFS(propertyFilePath)
    }
  }

  def loadPropertiesFromHDFS(filePath: String): scala.collection.mutable.Map[String, String] = {
    val lines = sparkSession.read.text(filePath).filter(line => line.toString().contains("="))
    val linesRDD: RDD[(String, String)] = lines.rdd.map(row => {
      val line = row.getString(0)
      val key = line.toString().split("=")(0).replace("[", "").trim()
      val value = line.toString().split("=")(1).replace("]", "").trim()
      (key, value)
    })
    return scala.collection.mutable.Map(linesRDD.collectAsMap().toSeq: _*)
  }

  def postProcess() {
    try {
      if (this.sparkSession != null) {
        this.sparkSession.close()
      }
    } catch {
      case e: Exception => println("Exception occured while closing spark session.")
    } finally {
      this.sparkSession = null
    }
  }

}