/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.codec

import java.nio.charset.StandardCharsets.UTF_8
import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.InstantFactory
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

import java.time.{ Instant, LocalDateTime }
import java.util.TimeZone

/**
 * INTERNAL API
 */
@InternalApi private[akka] sealed trait TimestampCodec {

  def encode(timestamp: Instant): Any
  def decode(row: Row, name: String): Instant
  def decode(row: Row, index: Int): Instant

  def instantNow(): Instant = InstantFactory.now()

  def now[T](): T
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TimestampCodec {
  case object PostgresTimestampCodec extends TimestampCodec {
    override def decode(row: Row, name: String): Instant = row.get(name, classOf[Instant])
    override def decode(row: Row, index: Int): Instant = row.get(index, classOf[Instant])

    override def encode(timestamp: Instant): Any = timestamp

    override def now[T](): T = instantNow().asInstanceOf[T]
  }

  case object SqlServerTimestampCodec extends TimestampCodec {

    // should this come from config?
    private val zone = TimeZone.getTimeZone("UTC").toZoneId

    private def toInstant(timestamp: LocalDateTime) =
      timestamp.atZone(zone).toInstant

    override def decode(row: Row, name: String): Instant = toInstant(row.get(name, classOf[LocalDateTime]))

    override def encode(timestamp: Instant): LocalDateTime = LocalDateTime.ofInstant(timestamp, zone)

    override def now[T](): T = {
      val fa: LocalDateTime = LocalDateTime.ofInstant(instantNow(), zone)
      println(s"now sql ${fa}")

      fa.asInstanceOf[T]
    }

    override def decode(row: Row, index: Int): Instant = toInstant(row.get(index, classOf[LocalDateTime]))
  }

  /**
   * maybe go here for a more type oriented naming, like "getInstant" or "bindInstant"
   */
  implicit class TimestampRichStatement[T](val statement: Statement)(implicit codec: TimestampCodec) extends AnyRef {
    def bindTimestamp(name: String, timestamp: Instant): Statement = statement.bind(name, codec.encode(timestamp))
    def bindTimestamp(index: Int, timestamp: Instant): Statement = statement.bind(index, codec.encode(timestamp))
  }
  implicit class TimestampRichRow[T](val row: Row)(implicit codec: TimestampCodec) extends AnyRef {
    //def getTimestamp(index: Int): Instant = codec.decode(row, index)
    def getTimestamp(index: String = "db_timestamp"): Instant = codec.decode(row, index)
  }
}
