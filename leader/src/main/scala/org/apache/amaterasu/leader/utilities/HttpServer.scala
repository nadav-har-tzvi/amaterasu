/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.leader.utilities

import org.apache.amaterasu.common.logging.Logging
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.eclipse.jetty.server.handler.{ErrorHandler, HandlerCollection, RequestLogHandler}
import org.eclipse.jetty.server.{NCSARequestLog, Server, ServerConnector}
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.log.StdErrLog

/**
  * Implementation of Jetty Web server to server Amaterasu libraries and other distribution files
  */
object HttpServer extends Logging {
  //private val logger = Logger.getLogger(HttpServer.getClass)
  var server: Server = _

  def start(port: String, serverRoot: String, host: String): Unit = {

    /*val threadPool = new QueuedThreadPool(Runtime.getRuntime.availableProcessors() * 16)
    threadPool.setName("Jetty")*/

    val requestLog = new NCSARequestLog("/tmp/jetty-yyyy_mm_dd.request.log")
    requestLog.setAppend(true)
    requestLog.setExtended(false)
    requestLog.setLogTimeZone("GMT")
    requestLog.setLogLatency(true)

    val handlers = new HandlerCollection()

    BasicConfigurator.configure()
    initLogging()
    server = new Server()
    server.setHandler(handlers)

    val requestLogHandler = new RequestLogHandler()
    requestLogHandler.setRequestLog(requestLog)
    handlers.addHandler(requestLogHandler)

    val connector = new ServerConnector(server)
    connector.setPort(port.toInt)
    connector.setHost(host)
    server.addConnector(connector)
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    println("--------------------------")
    println(serverRoot)
    context.setResourceBase(serverRoot)
    context.setContextPath("/")
    handlers.addHandler(context)

    context.setErrorHandler(new ErrorHandler())
    context.setInitParameter("dirAllowed", "true")
    context.setInitParameter("pathInfoOnly", "true")
    context.addServlet(new ServletHolder(new DefaultServlet()), "/")

    server.start()
  }

  def stop() {
    if (server == null) throw new IllegalStateException("Server not started")

    server.stop()
    server = null
  }

  def initLogging(): Unit = {
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[StdErrLog].getName)
    Logger.getLogger("org.eclipse.jetty").setLevel(Level.DEBUG)
    Logger.getLogger("org.eclipse.jetty.websocket").setLevel(Level.DEBUG)
  }
}
