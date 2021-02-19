package com.ncr.edl.apollo.common

import java.sql.Timestamp
import java.util.Date
import java.util.concurrent.Executors
import java.util.concurrent.Executors._
import java.util.concurrent.ThreadPoolExecutor
import scala.reflect.runtime.universe
import scala.util.control.Breaks
import java.util.concurrent.TimeUnit
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CancellationException
import java.util.concurrent.Future
import java.util.concurrent.ExecutionException
import java.util.concurrent.ArrayBlockingQueue
import java.lang.reflect.InvocationTargetException

object CommonSparkDriver {

  def main(args: Array[String]) {

    if (args.length < 4) {
      println("Property file path not provided and Execution File Order not provided.")
      println("Sample: conf/db-param/d8-db-params.properties \"com.ncr.edl.apollo.install_base_addr_dn\" \"d8ibasepartydn07:1,d8ibasepartydn07:1 ,d8ibasepartydn07:2,d8ibasepartydn07:3,d8ibasepartydn07:3\" \"5\" activityCountRequired")
      sys.exit(1)
    }
    val propertyFilePath = args(0)
    val packageHierachy = args(1).trim()
    val fileExecutionOrder = args(2).trim()
    val poolSize = args(3).trim()
    var activityCountRequired = false
    if (args.length > 4) {
      activityCountRequired = args(4).toBoolean
    }

    val executor = new ExecutorDao(activityCountRequired, packageHierachy)
    val dbParams = executor.loadProperties(propertyFilePath)

    val job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'CommonSparkDriver' at %s.".format(job_start_time.toString()))

    try {

      val groups = scala.collection.mutable.Map[String, String]()
      val fileGroups = fileExecutionOrder.split(",");
      fileGroups.foreach(fileGroup => {
        val split = fileGroup.split(":");
        var values = "";
        if (groups.get(split(1).trim()) != None) {
          values = groups.get(split(1).trim()).get;
        }
        values = values + "," + split(0).trim()
        groups.put(split(1).trim(), values)
      })

      var taskCounter = 0L
      val sortedGroups = collection.immutable.SortedSet[Int]() ++ groups.keySet.map(key => key.toInt)
      for (x <- sortedGroups) {
        println("Executing group: " + x)
        val files = groups.get(x.toString()).get.split(",")
        val threadExecutor: ThreadPoolExecutor = newFixedThreadPool(poolSize.toInt).asInstanceOf[ThreadPoolExecutor]
        files.foreach(file => {
          if (file != null && file.trim() != "") {
            println("Added file %s to group %s".format(file, x))
            val handler = new Handler(executor, dbParams, file, packageHierachy, "run")
            executor.setCurrentGroup(x.toString())
            threadExecutor.execute(handler)
          }
        })
        val loop = new Breaks;
        loop.breakable {
          while (true) {
            Thread.sleep(10000)
            if (threadExecutor.getActiveCount() == 0) {
              println("Group :" + x + " Execution Completed")
              loop.break
            }
          }
        }
        taskCounter += threadExecutor.getCompletedTaskCount
        threadExecutor.shutdown()
      }

      println("Total number Completed Files :" + taskCounter);
    } catch {
      case e: java.lang.Exception =>
        e.printStackTrace()
        System.exit(1)
      case e: java.lang.reflect.InvocationTargetException =>
        e.printStackTrace()
        System.exit(1)
      case e: java.lang.RuntimeException =>
        e.printStackTrace()
        System.exit(1)
      case e: java.lang.Throwable =>
        e.printStackTrace()
        System.exit(1)
      case e: Exception =>
        e.printStackTrace()
        System.exit(1)
    } finally {
      executor.postProcess()
      println("################################################################")
    }
  }

  class Handler(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String], className: String, packageHierarchy: String, method: String) extends Runnable {

    def run() {
      println("Execution Started for the File:" + className)
      val executorClass = Class.forName(if (!packageHierarchy.trim().startsWith("com.")) className else (packageHierarchy + "." + className))
      val objectInstance = executorClass.getConstructor(classOf[ExecutorDao], classOf[scala.collection.mutable.Map[String, String]])
        .newInstance(executor, dbParams)
      val methodInstance = objectInstance.getClass().getMethod(method)
      try {
        methodInstance.invoke(objectInstance)
      } catch {
        case e: java.lang.Exception =>
          e.printStackTrace()
          System.exit(1)
        case e: java.lang.reflect.InvocationTargetException =>
          e.printStackTrace()
          System.exit(1)
        case e: java.lang.RuntimeException =>
          e.printStackTrace()
          System.exit(1)
        case e: java.lang.Throwable =>
          e.printStackTrace()
          System.exit(1)
      }

      println("Execution Completed for the File: " + className)
    }
  }
}

