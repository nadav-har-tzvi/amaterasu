package io.shinto.amaterasu.common.execution.dependencies

import scala.collection.mutable.ListBuffer

/**
  * Created by roadan on 8/28/16.
  */
case class Dependencies(repos: ListBuffer[Repo], artifacts: List[Artifact], pythonPackages: Option[List[PythonPackage]] = None, condaReqFile: Option[PythonReqFile] = None, pipReqFile: Option[PythonReqFile] = None)
case class Repo(id: String, `type`: String, url: String)
case class Artifact(groupId: String, artifactId: String, version: String)
case class PythonPackage(packageId: String, index: Option[String] = None, channel: Option[String] = None) // Not really sure about this, basically I want default values but the ObjectMapper apparently doesn't support them
case class PythonReqFile(path: String)