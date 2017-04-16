package io.shinto.amaterasu.leader.mesos.executors

import java.nio.file.{Files, Paths}

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by nadav-har-tzvi on 4/15/17.
  */
class DataLoaderTests extends FlatSpec with Matchers {

  "A parsed jars file with only conda packages" should "result with a full list of conda dependencies" in {
    var url = getClass.getResource("/jars-only-conda1.yml")
    val path = url.getPath
    val dependencies = DataLoader.resolveDependencies(path)
    dependencies.condaPackages.length should not be 0
  }

  "A parsed jars file with only conda req file" should "result in a dependencies that include a reqFile path" in {
    var url = getClass.getResource("/jars-only-conda2.yml")
    val path = url.getPath
    val dependencies = DataLoader.resolveDependencies(path)
    dependencies.condaReqFile.path should not be null
  }

}
