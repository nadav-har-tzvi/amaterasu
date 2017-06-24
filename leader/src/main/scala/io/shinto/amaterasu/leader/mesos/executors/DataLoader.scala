package io.shinto.amaterasu.leader.mesos.executors

import java.io.File
import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.execution.dependencies.{Artifact, Dependencies, PythonPackage, Repo}
import io.shinto.amaterasu.common.runtime.Environment
import org.apache.mesos.protobuf.ByteString
import io.shinto.amaterasu.common.dataobjects._

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by karel_alfonso on 27/06/2016.
  */
object DataLoader extends Logging {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def getTaskData(actionData: ActionData, env: String): ByteString = {

    val srcFile = actionData.src
    val src = Source.fromFile(s"repo/src/${srcFile}").mkString
    val envValue = Source.fromFile(s"repo/env/${env}.json").mkString

    val envData = mapper.readValue(envValue, classOf[Environment])

    val data = mapper.writeValueAsBytes(new TaskData(src, envData, actionData.groupId, actionData.typeId))
    ByteString.copyFrom(data)

  }

  def resolveDependencies(dependenciesFilePath: String) : Dependencies = {
    val ymlMapper = new ObjectMapper(new YAMLFactory())
    ymlMapper.registerModule(DefaultScalaModule)
    var depsData: Dependencies = null
    if (Files.exists(Paths.get(dependenciesFilePath))) {
      val depsValue = Source.fromFile(dependenciesFilePath).mkString
      depsData = ymlMapper.readValue(depsValue, classOf[Dependencies])
    }
    depsData
  }

  /**
    * Takes in a list of dependencies and merges them into a single Dependency object
    * @param allDepsData: A collection of independent dependencies
    * @return A single Dependencies object that contains the contents of supplied dependencies
    */
  def mergeDependencies(allDepsData: List[Dependencies]): Dependencies = {
    val reps = new ListBuffer[Repo]
    val artifacts = new ListBuffer[Artifact]
    val pyPackages = new ListBuffer[PythonPackage]
    for (dep <- allDepsData) {
      reps ++= dep.repos.getOrElse(new ListBuffer[Repo])
      artifacts ++= dep.artifacts.getOrElse(new ListBuffer[Artifact])
      pyPackages ++= dep.pythonPackages.getOrElse(new ListBuffer[PythonPackage])
    }
    val mergedDeps = Dependencies(Option(reps), Option(artifacts.toList), Option(pyPackages.toList))
    mergedDeps
  }

  def getExecutorData(env: String): ByteString = {

    val ymlMapper = new ObjectMapper(new YAMLFactory())
    ymlMapper.registerModule(DefaultScalaModule)

    val envValue = Source.fromFile(s"repo/env/${env}.json").mkString
    val envData = mapper.readValue(envValue, classOf[Environment])
    val depFiles = new File("repo/deps").listFiles.filter(_.isFile).toList
    val allDepsData = for (f <- depFiles) yield resolveDependencies(f.getAbsolutePath)
    val mergedDepsData = mergeDependencies(allDepsData)
    val data = mapper.writeValueAsBytes(new ExecData(envData, mergedDepsData))
    ByteString.copyFrom(data)
  }

}