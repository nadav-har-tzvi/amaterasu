package io.shinto.amaterasu.executor.execution.actions.runners.spark

import java.io.{ByteArrayOutputStream, File, PrintWriter, StringWriter}

import io.shinto.amaterasu.common.dataobjects.ExecData
import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.execution.dependencies.{Artifact, Dependencies, PythonPackage, Repo}
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.sdk.{AmaterasuRunner, RunnersProvider}
import io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner
import org.apache.spark.repl.amaterasu.ReplUtils
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.eclipse.aether.util.artifact.JavaScopes
import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact
import com.jcabi.aether.Aether

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import sys.process._

/**
  * Created by roadan on 2/9/17.
  */
class SparkRunnersProvider extends RunnersProvider with Logging {

  private val runners = new TrieMap[String, AmaterasuRunner]
  private val shellLogger = ProcessLogger(
    (o: String) => log.info(o),
    (e: String) => log.error(e)
  )

  override def init(data: ExecData, jobId: String, outStream: ByteArrayOutputStream, notifier: Notifier, executorId: String) : Unit = {

    var jars = Seq[String]()
    if (data.deps != null) {
      jars ++= getDependencies(data.deps)
    }

    val classServerUri = ReplUtils.getOrCreateClassServerUri(outStream, jars)

    val sparkAppName = s"job_${jobId}_executor_$executorId"
    log.info(s"creating SparkContext with master ${data.env.master}")
    val sparkContext = SparkRunnerHelper.createSparkContext(data.env, sparkAppName, classServerUri, jars)

    val sparkScalaRunner = SparkScalaRunner(data.env, jobId, sparkContext, outStream, notifier, jars)
    sparkScalaRunner.initializeAmaContext(data.env)

    runners.put(sparkScalaRunner.getIdentifier, sparkScalaRunner)

    val pySparkRunner = PySparkRunner(data.env, jobId, notifier, sparkContext, "spark-1.6.1-2/python/pyspark")
    runners.put(pySparkRunner.getIdentifier(), pySparkRunner)
  }

  override def getGroupIdentifier: String = "spark"

  override def getRunner(id: String): AmaterasuRunner = runners(id)

  private def installAnacondaPackage(pythonPackage: PythonPackage): Unit = {
    log.info(s"installAnacondaPackage: $pythonPackage")
    val channel = pythonPackage.channel.getOrElse("anaconda")
    if (channel == "anaconda") {
      Seq("bash", "-c", s"$$PWD/miniconda/bin/python -m conda install -y ${pythonPackage.packageId}") ! shellLogger
    } else {
      Seq("bash", "-c", s"$$PWD/miniconda/bin/python -m conda install -y -c $channel ${pythonPackage.packageId}") ! shellLogger
    }
  }

  // TODO: consider adding PyPi, for some reason skeleton is broken
//  private def installPyPiPackage(pythonPackage: PythonPackage): Unit = {
//    log.warn(s"PyPi Packages are not supported yet. If you really need this feature, please contact us!")
////    Seq("bash", "-c", s"$$PWD/miniconda/bin/python -m conda skeleton pypi ${pythonPackage.packageId}") ! shellLogger
////    Seq("bash", "-c", s"$$PWD/miniconda/bin/python -m conda build ${pythonPackage.packageId}") ! shellLogger
//  }

  private def installAnacondaOnNode(): Unit = {
    log.debug(s"Preparing to install Miniconda")
    Seq("bash", "-c", "sh Miniconda2-latest-Linux-x86_64.sh -b -p $PWD/miniconda") ! shellLogger
    Seq("bash", "-c", "$PWD/miniconda/bin/python -m conda install -y conda-build") ! shellLogger
    Seq("bash", "-c", "$PWD/miniconda/bin/python -m conda update -y") ! shellLogger
    Seq("bash", "-c", "ln -s $PWD/spark-1.6.1-2/python/pyspark $PWD/miniconda/pkgs/pyspark") ! shellLogger

  }

  private def loadPythonDependencies(deps: Dependencies) = {
    installAnacondaOnNode()
    val py4jPackage = PythonPackage("py4j", channel=Option("conda-forge"))
    installAnacondaPackage(py4jPackage)
    val codegenPackage = PythonPackage("codegen", channel=Option("auto"))
    installAnacondaPackage(codegenPackage)
    if (deps.pythonPackages.isDefined) {
      try {
        log.info(s"deps: $deps")
        deps.pythonPackages.head.foreach(pack => {
          log.info(s"PyPackage: $pack, index: ${pack.index}")
          pack.index.getOrElse("anaconda").toLowerCase match {
            case "anaconda" => installAnacondaPackage(pack)
//            case "pypi" => installPyPiPackage(pack)
          }
        })
      }
      catch {
        case rte: RuntimeException =>
          val sw = new StringWriter
          rte.printStackTrace(new PrintWriter(sw))
          log.error(s"Failed to activate environment (runtime) - cause: ${rte.getCause}, message: ${rte.getMessage}, Stack: \n${sw.toString}")
        case e: Exception =>
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          log.error(s"Failed to activate environment (other) - type: ${e.getClass.getName}, cause: ${e.getCause}, message: ${e.getMessage}, Stack: \n${sw.toString}")
      }
    }

  }

  private def getDependencies(deps: Dependencies): Seq[String] = {

    // adding a local repo because Aether needs one
    val repo = new File(System.getProperty("java.io.tmpdir"), "ama-repo")

    val remotes = deps.repos.getOrElse(new ListBuffer[Repo]).map(r =>
      new RemoteRepository(
        r.id,
        r.`type`,
        r.url
      )).toList.asJava

    val aether = new Aether(remotes, repo)
    loadPythonDependencies(deps)

    deps.artifacts.getOrElse(new ListBuffer[Artifact]).flatMap(a => {
      aether.resolve(
        new DefaultArtifact(a.groupId, a.artifactId, "", "jar", a.version),
        JavaScopes.RUNTIME
      ).map(a => a) // .toBuffer[Artifact]
    }).map(x => x.getFile.getAbsolutePath)


  }
}