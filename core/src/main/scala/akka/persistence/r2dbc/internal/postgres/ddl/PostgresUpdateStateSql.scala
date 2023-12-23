/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.ddl

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.ddl.PostgresSqlHelper.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.internal.{ DurableStateDao, PayloadCodec }
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import io.r2dbc.spi.Statement

import scala.collection.immutable
import java.lang

class PostgresUpdateStateSql(settings: R2dbcSettings)(implicit val statePayloadCodec: PayloadCodec)
    extends AbstractUpdateStateSql
    with PostgresSqlHelper
    with BaseSqlHelper {

  private def additionalUpdateParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(col, _: AdditionalColumn.BindValue[_]) =>
          strB.append(", ").append(col.columnName).append(" = ?")
        case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
          strB.append(", ").append(col.columnName).append(" = ?")
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  override def updateStateSql(
      entityType: String,
      updateTags: Boolean,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)

    val timestamp =
      if (settings.dbTimestampMonotonicIncreasing)
        "CURRENT_TIMESTAMP"
      else
        "GREATEST(CURRENT_TIMESTAMP, " +
        s"(SELECT db_timestamp + '1 microsecond'::interval FROM $stateTable WHERE persistence_id = ? AND revision = ?))"

    val revisionCondition =
      if (settings.durableStateAssertSingleWriter) " AND revision = ?"
      else ""

    val tags = if (updateTags) ", tags = ?" else ""

    val additionalParams = additionalUpdateParameters(additionalBindings)
    sql"""
      UPDATE $stateTable
      SET revision = ?, state_ser_id = ?, state_ser_manifest = ?, state_payload = ?$tags$additionalParams, db_timestamp = $timestamp
      WHERE persistence_id = ?
      $revisionCondition"""
  }

  override def bindForUpdateStateSql(
      stmt: Statement,
      getAndIncIndex: () => Int,
      state: SerializedStateRow,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings],
      previousRevision: Long): Statement = {

    stmt
      .bind(getAndIncIndex(), state.revision)
      .bind(getAndIncIndex(), state.serId)
      .bind(getAndIncIndex(), state.serManifest)
      .bindPayloadOption(getAndIncIndex(), state.payload)
    bindTags(stmt, getAndIncIndex(), state)
    bindAdditionalColumns(getAndIncIndex, stmt, additionalBindings)

    if (settings.dbTimestampMonotonicIncreasing) {
      if (settings.durableStateAssertSingleWriter)
        stmt
          .bind(getAndIncIndex(), state.persistenceId)
          .bind(getAndIncIndex(), previousRevision)
      else
        stmt
          .bind(getAndIncIndex(), state.persistenceId)
    } else {
      stmt
        .bind(getAndIncIndex(), state.persistenceId)
        .bind(getAndIncIndex(), previousRevision)
        .bind(getAndIncIndex(), state.persistenceId)

      if (settings.durableStateAssertSingleWriter)
        stmt.bind(getAndIncIndex(), previousRevision)
      else
        stmt
    }
  }

  override def bindForDeleteState(
      stmt: Statement,
      revision: Long,
      persistenceId: String,
      previousRevision: Long): Statement = {
    stmt
      .bind(0, revision)
      .bind(1, 0)
      .bind(2, "")
      .bindPayloadOption(3, None)

    if (settings.dbTimestampMonotonicIncreasing) {
      if (settings.durableStateAssertSingleWriter)
        stmt
          .bind(4, persistenceId)
          .bind(5, previousRevision)
      else
        stmt
          .bind(4, persistenceId)
    } else {
      stmt
        .bind(4, persistenceId)
        .bind(5, previousRevision)
        .bind(6, persistenceId)

      if (settings.durableStateAssertSingleWriter)
        stmt.bind(7, previousRevision)
      else
        stmt
    }
  }
}
