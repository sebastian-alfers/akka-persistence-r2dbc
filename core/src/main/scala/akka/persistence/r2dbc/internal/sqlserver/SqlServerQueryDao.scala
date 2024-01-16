/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.internal.postgres.PostgresQueryDao
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampRichStatement
import io.r2dbc.spi.{ ConnectionFactory, Statement }
import org.slf4j.{ Logger, LoggerFactory }

import java.time.{ Instant, LocalDateTime }
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerQueryDao {
  private def log: Logger = LoggerFactory.getLogger(classOf[SqlServerQueryDao])

}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class SqlServerQueryDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresQueryDao(settings, connectionFactory) {}
