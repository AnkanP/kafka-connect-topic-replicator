package org.example

import org.slf4j.LoggerFactory

import java.io.InputStream
import java.util.Properties

class Version{

}

object Version{

  private val log = LoggerFactory.getLogger(classOf[Version])

  private val PATH = "/kafka-sink-version.properties"
  private var version = "unknown"

  try {

    val stream: InputStream = classOf[Version].getResourceAsStream(PATH)
    val props = new Properties()
    props.load(stream)
    version = props.getProperty("version", version).trim
  }
    catch {
      case e: Exception =>
      //log.warn("VERSION LOADING ERROR!!!!:", e)
    }


  def getVersion: String = version

}