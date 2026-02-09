package com.sepavliuk.capstone.logging

import cats.effect.IO
import org.slf4j.{Logger, LoggerFactory, MDC}

object Log {
  def apply(clazz: Class[_]): Log = new Log(clazz.getName)
  def apply(className: String): Log = new Log(className)
}

class Log(val name: String) extends Serializable {

  @transient private lazy val logger: Logger = LoggerFactory.getLogger(name)

  def info(message: String): IO[Unit] = IO(infoUnsafe(message))
  def warn(message: String): IO[Unit] = IO(warnUnsafe(message))
  def debug(message: String): IO[Unit] = IO(debugUnsafe(message))
  def error(message: String, ex: Throwable): IO[Unit] = IO(logger.error(message, ex))
  def error(message: String, exOpt: Option[Throwable]): IO[Unit] = exOpt match {
    case Some(ex) => IO(logger.error(message, ex))
    case None     => IO(logger.error(message))
  }
  def error(message: String): IO[Unit] = IO(logger.error(message))

  def infoUnsafe(message: String): Unit = logger.info(s"${MDC.get("app_name")} $message")
  def warnUnsafe(message: String): Unit = logger.warn(s"${MDC.get("app_name")} $message")
  def debugUnsafe(message: String): Unit = logger.debug(message)
  def errorUnsafe(message: String): Unit = logger.error(message)

  def innerLogger: Logger = logger
}
