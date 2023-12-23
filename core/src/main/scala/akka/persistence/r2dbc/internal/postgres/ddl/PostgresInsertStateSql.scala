/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.ddl

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.{ DurableStateDao, PayloadCodec }
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement

import java.lang
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.ddl.PostgresSqlHelper.EvaluatedAdditionalColumnBindings
import io.r2dbc.spi.Statement

import scala.collection.immutable

class PostgresInsertStateSql(settings: R2dbcSettings)(implicit val statePayloadCodec: PayloadCodec)
    extends AbstractInsertStateSql
    with PostgresSqlHelper
    with BaseSqlHelper {

  def bindForUpsertState(
      stmt: Statement,
      getAndIncIndex: () => Int,
      slice: Int,
      entityType: String,
      state: DurableStateDao.SerializedStateRow,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings]): _root_.io.r2dbc.spi.Statement = {
    stmt
      .bind(getAndIncIndex(), slice)
      .bind(getAndIncIndex(), entityType)
      .bind(getAndIncIndex(), state.persistenceId)
      .bind(getAndIncIndex(), state.revision)
      .bind(getAndIncIndex(), state.serId)
      .bind(getAndIncIndex(), state.serManifest)
      .bindPayloadOption(getAndIncIndex(), state.payload)
    bindTags(stmt, getAndIncIndex(), state)
    bindAdditionalColumns(getAndIncIndex, stmt, additionalBindings)
  }

  def bindForDeleteState(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      revision: Long): _root_.io.r2dbc.spi.Statement = {
    stmt
      .bind(0, slice)
      .bind(1, entityType)
      .bind(2, persistenceId)
      .bind(3, revision)
      .bind(4, 0)
      .bind(5, "")
      .bindPayloadOption(6, None)
      .bindNull(7, classOf[Array[String]])
  }

  override def insertStateSql(
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    val stateTable = settings.getDurableStateTableWithSchema(entityType)
    val additionalCols = additionalInsertColumns(additionalBindings)
    val additionalParams = additionalInsertParameters(additionalBindings)
    sql"""
    INSERT INTO $stateTable
    (slice, entity_type, persistence_id, revision, state_ser_id, state_ser_manifest, state_payload, tags$additionalCols, db_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?$additionalParams, CURRENT_TIMESTAMP)"""
  }

}
