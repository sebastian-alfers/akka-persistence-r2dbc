/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.ddl

import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.DurableStateDao.SerializedStateRow
import akka.persistence.r2dbc.internal.{ DurableStateDao, PayloadCodec }
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import akka.persistence.r2dbc.internal.PayloadCodec.RichStatement

import java.lang
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.internal.postgres.ddl.PostgresSqlHelper.EvaluatedAdditionalColumnBindings
import io.r2dbc.spi.Statement

import scala.collection.immutable

trait AbstractUpdateStateSql {
  def bindForDeleteState(stmt: Statement, revision: Long, persistenceId: String, previousRevision: Long): Statement

  def bindForUpdateStateSql(
      stmt: Statement,
      getAndIncIndex: () => Int,
      state: SerializedStateRow,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings],
      previousRevision: Long): Statement

  def updateStateSql(
      entityType: String,
      updateTags: Boolean,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String
}
