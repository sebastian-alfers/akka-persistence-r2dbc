/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.sqlserver.ddl

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.{ DurableStateDao, PayloadCodec }
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement

import java.lang
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.ddl.{ AbstractInsertStateSql, BaseSqlHelper }
import akka.persistence.r2dbc.internal.postgres.ddl.PostgresSqlHelper.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.internal.sqlserver.SqlServerDialectHelper
import io.r2dbc.spi.Statement

import scala.collection.immutable

class SqlServerInsertStateSql(settings: R2dbcSettings)(implicit val statePayloadCodec: PayloadCodec)
    extends AbstractInsertStateSql
    with BaseSqlHelper {

  // can probably extended
  private val helper = SqlServerDialectHelper(settings.connectionFactorySettings.config)

  import helper._

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
      .bind(getAndIncIndex(), nowLocalDateTime())
    bindTags(stmt, getAndIncIndex(), state)
    bindAdditionalColumns(getAndIncIndex, stmt, additionalBindings)
    //    stmt
    //      .bind(getAndIncIndex(), slice)
    //      .bind(getAndIncIndex(), entityType)
    //      .bind(getAndIncIndex(), state.persistenceId)
    //      .bind(getAndIncIndex(), state.revision)
    //      .bind(getAndIncIndex(), state.serId)
    //      .bind(getAndIncIndex(), state.serManifest)
    //      .bindPayloadOption(getAndIncIndex(), state.payload)
    //    bindTags(stmt, getAndIncIndex(), state)
    //    bindAdditionalColumns(getAndIncIndex, stmt, additionalBindings)
    //    stmt.bind(getAndIncIndex(), nowLocalDateTime())
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
  VALUES (@slice, @entityType, @persistenceId, @revision, @stateSerId, @stateSerManifest, @statePayload, @tags$additionalParams, @now)"""
  }

  override def bindForDeleteState(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      revision: Long): Statement = {
    stmt
      .bind("@slice", slice)
      .bind("@entityType", entityType)
      .bind("@persistenceId", persistenceId)
      .bind("@revision", revision)
      .bind("@stateSerId", 0)
      .bind("@stateSerManifest", "")
      .bindPayloadOption("@statePayload", None)
      .bindNull("@tags", classOf[String])
      .bind("@now", nowLocalDateTime())

  }
}
