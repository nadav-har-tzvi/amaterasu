package io.shinto.amaterasu.leader.mesos.executors

import java.nio.file.{Files, Paths}

import io.shinto.amaterasu.common.execution.dependencies.{Artifact, Dependencies, PythonPackage, Repo}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by nadav-har-tzvi on 4/15/17.
  */
class DataLoaderTests extends FlatSpec with Matchers {

  "A parsed jars file with only conda packages" should "result with a full list of conda dependencies" in {
    var url = getClass.getResource("/jars-only-conda1.yml")
    val path = url.getPath
    val dependencies = DataLoader.resolveDependencies(path)
    dependencies.pythonPackages.head.length should not be 0
  }

  "Multiple dependency files" should "be merged into one dependency file" in {
    val dep1 = Dependencies(new ListBuffer[Repo], new ListBuffer[Artifact].toList, Option(new ListBuffer[PythonPackage].toList))
    val pyPackages = new ListBuffer[PythonPackage]
    pyPackages += PythonPackage("Django")
    val dep2 = Dependencies(new ListBuffer[Repo], new ListBuffer[Artifact].toList, Option(pyPackages.toList))
    val allDeps = new ListBuffer[Dependencies]
    allDeps += dep1
    allDeps += dep2
    val mergedDeps = DataLoader.mergeDependencies(allDeps.toList)
    mergedDeps.pythonPackages.head.head.packageId should be "Django"
  }

}
