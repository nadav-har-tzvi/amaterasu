package io.shinto.amaterasu.dsl

import java.util.concurrent.BlockingQueue
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{ JsonNode, ObjectMapper }
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.execution.JobManager
import io.shinto.amaterasu.execution.actions.{ ErrorAction, Action, SequentialAction }

import org.apache.curator.framework.CuratorFramework

import scala.io.Source

/**
  * The JobParser class is in charge of parsing the maki.yaml file which
  * describes the workflow of an amaterasu job
  */
object JobParser {

  def loadMakiFile(): String = {

    Source.fromFile("repo/maki.yaml").mkString

  }

  /**
    * Parses the maki.yaml string and creates a job manager
    * @param jobId
    * @param maki a string containing the YAML definition of the job
    * @param actionsQueue
    * @param client
    * @return
    */
  def parse(
    jobId: String,
    maki: String,
    actionsQueue: BlockingQueue[ActionData],
    client: CuratorFramework
  ): JobManager = {

    val mapper = new ObjectMapper(new YAMLFactory())

    val job = mapper.readTree(maki)

    // loading the job details
    val manager = JobManager(jobId, job.path("job-name").asText, actionsQueue, client)

    // iterating the flow list and constructing the job's flow
    val actions = job.path("flow").asInstanceOf[ArrayNode].asScala.toSeq

    parseActions(actions, manager, actionsQueue, null)

    manager
  }

  /**
    * parseActions is a recursive function, for building the workflow of
    * the job
    * God, I miss Clojure
    * @param actions a seq containing the definitions of all the actions
    * @param manager the job manager for the job
    * @param actionsQueue
    * @param previous the previous action, this is used in order to add the current action
    *                 to the nextActionIds
    */
  def parseActions(
    actions: Seq[JsonNode],
    manager: JobManager,
    actionsQueue: BlockingQueue[ActionData],
    previous: Action
  ): Unit = {

    if (actions.isEmpty)
      return

    val actionData = actions.head

    val action = parseSequentialAction(
      actionData,
      manager.jobId,
      actionsQueue,
      manager.client
    )

    if (previous != null)
      previous.data.nextActionIds.append(action.actionId)

    manager.registerAction(action)

    val errorNode = actionData.path("error")

    if (!errorNode.isMissingNode) {

      val errorAction = parseErrorAction(
        errorNode,
        manager.jobId,
        action.data.id,
        actionsQueue,
        manager.client
      )

      action.data.errorActionId = errorAction.data.id
      manager.registerAction(errorAction)
    }

    parseActions(actions.tail, manager, actionsQueue, action)

  }

  def parseSequentialAction(
    action: JsonNode,
    jobId: String,
    actionsQueue: BlockingQueue[ActionData],
    client: CuratorFramework
  ): SequentialAction = {

    SequentialAction(
      action.path("name").asText,
      action.path("src").asText,
      action.path("type").asText,
      jobId,
      actionsQueue,
      client
    )

  }

  def parseErrorAction(
    action: JsonNode,
    jobId: String,
    parent: String,
    actionsQueue: BlockingQueue[ActionData],
    client: CuratorFramework
  ): SequentialAction = {

    ErrorAction(
      action.path("name").asText,
      action.path("src").asText,
      parent,
      action.path("type").asText,
      jobId,
      actionsQueue,
      client
    )

  }
}