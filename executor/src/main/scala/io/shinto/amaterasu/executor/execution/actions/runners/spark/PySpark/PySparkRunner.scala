package io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark

import java.io.{File, OutputStream, PrintWriter}

import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.sdk.AmaterasuRunner
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.sql.SQLContext

import scala.sys.process.{BasicIO, Process, ProcessIO}
import scala.io.Source

/**
  * Created by roadan on 9/2/16.
  */
class PySparkRunner extends AmaterasuRunner with Logging {

  var proc: Process = null
  var notifier: Notifier = null

  override def getIdentifier(): String = "pyspark"

  override def executeSource(sourcePath: String, actionName: String): Unit = {
    val f = new File("/tmp/source.txt")
    val pw = new PrintWriter(f)
    pw.println(sourcePath)
    pw.println(actionName)
    pw.close()
    notifier.info(s"executeSource { sourcePath: $sourcePath, actionName: $actionName}")
//    val source = Source.fromFile(sourcePath).getLines().mkString("\n")
    interpretSources(sourcePath, actionName)
  }

  def interpretSources(source: String, actionName: String) = {
    notifier.info(s"interpretSources { source: $source, actionName: $actionName}")
    PySparkEntryPoint.getExecutionQueue().setForExec((source, actionName))
    val resQueue = PySparkEntryPoint.getResultQueue(actionName)

    notifier.info(s"================= started action $actionName =================")

    var res: PySparkResult = null

    do {
      res = resQueue.getNext()
      notifier.info(s"res: $res")
      res.resultType match {
        case ResultType.success =>
          notifier.success(res.statement)
        case ResultType.error =>
          notifier.error(res.statement, res.message)
          throw new Exception(res.message)
        case ResultType.completion =>
          notifier.info(s"================= finished action $actionName =================")
      }
    } while (res != null && res.resultType != ResultType.completion)
  }

}

object PySparkRunner {

  def apply(env: Environment,
            jobId: String,
            notifier: Notifier,
            sc: SparkContext,
            pypath: String): PySparkRunner = {

    val result = new PySparkRunner

    PySparkEntryPoint.start(sc, jobId, env, SparkEnv.get)
    val port = PySparkEntryPoint.getPort()
    val proc = Process(Seq("spark-1.6.1-2/bin/pyspark", "spark_intp.py", port.toString), None,
      "PYTHONPATH" -> pypath,
      "PYSPARK_PYTHON" -> "miniconda/bin/python",
      "PYTHONHASHSEED" -> 0.toString) #> System.out
    notifier.info(s"pyspark process $proc")
    proc.run()


    result.notifier = notifier

    result
  }

}
