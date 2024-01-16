/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.postgres.PostgresJournalDao
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import io.r2dbc.spi.{ ConnectionFactory, Row, Statement }
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.concurrent.{ ExecutionContext, Future }

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerJournalDao {
  private def log: Logger = LoggerFactory.getLogger(classOf[SqlServerJournalDao])
  val TRUE = "1"
}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class SqlServerJournalDao(settings: R2dbcSettings, connectionFactory: ConnectionFactory)(implicit
    ec: ExecutionContext,
    system: ActorSystem[_])
    extends PostgresJournalDao(settings, connectionFactory) {}
