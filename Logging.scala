package com.cox

import org.slf4j.{ Logger, LoggerFactory }

/*
 * 
 */
trait Logging {
  protected def logger: Logger
}

/*
 * 
 */
trait LazyLogging extends Logging {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}