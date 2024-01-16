/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver

import java.lang
import scala.collection.immutable
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.{ QuerySettings, R2dbcSettings }
import akka.persistence.r2dbc.internal.Dialect
import akka.persistence.r2dbc.internal.codec.TimestampCodec
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampRichStatement
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.PostgresDurableStateDao.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import io.r2dbc.spi.{ ConnectionFactory, Statement }
import org.slf4j.{ Logger, LoggerFactory }

import java.time.Instant
import scala.concurrent.duration.{ Duration, FiniteDuration }
import scala.concurrent.{ ExecutionContext, Future }

/**
 * INTERNAL API
 */
@InternalApi
private[r2dbc] object SqlServerDurableStateDao {
  private def log: Logger = LoggerFactory.getLogger(classOf[SqlServerDurableStateDao])

}

/**
 * INTERNAL API
 *
 * Class for doing db interaction outside of an actor to avoid mistakes in future callbacks
 */
@InternalApi
private[r2dbc] class SqlServerDurableStateDao(
    settings: R2dbcSettings,
    connectionFactory: ConnectionFactory,
    dialect: Dialect)(implicit ec: ExecutionContext, system: ActorSystem[_])
    extends PostgresDurableStateDao(settings, connectionFactory, dialect) {

  override protected def selectStateSql(entityType: String): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    sql"""
    SELECT revision, state_ser_id, state_ser_manifest, state_payload, db_timestamp
    FROM $stateTable WHERE persistence_id = @persistenceId"""
  }

  override protected def insertStateSql(
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    val additionalCols = additionalInsertColumns(additionalBindings)
    val additionalParams = additionalInsertParameters(additionalBindings)
    sql"""
      INSERT INTO $stateTable
      (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags$additionalCols, db_timestamp)
      VALUES (@slice, @entityType, @persistenceId, @revision, @stateSerId, @stateSerManifest, @statePayload, @tags$additionalParams, @now)"""
  }

  override protected def additionalInsertParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(col, bindValue) if bindValue != AdditionalColumn.Skip =>
          strB.append(s", @${col.columnName}")
        case EvaluatedAdditionalColumnBindings(_, _) =>
      }
      strB.toString
    }
  }

}
