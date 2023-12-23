/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.ddl

import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.postgres.ddl.PostgresSqlHelper.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import io.r2dbc.spi.Statement

import java.lang
import scala.collection.immutable

trait AbstractInsertStateSql {

  def insertStateSql(
      entityType: String,
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String

  protected def additionalInsertParameters(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(_, _: AdditionalColumn.BindValue[_]) |
            EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindNull) =>
          strB.append(", ?")
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  protected def additionalInsertColumns(
      additionalBindings: immutable.IndexedSeq[EvaluatedAdditionalColumnBindings]): String = {
    if (additionalBindings.isEmpty) ""
    else {
      val strB = new lang.StringBuilder()
      additionalBindings.foreach {
        case EvaluatedAdditionalColumnBindings(c, _: AdditionalColumn.BindValue[_]) =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(c, AdditionalColumn.BindNull) =>
          strB.append(", ").append(c.columnName)
        case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
      }
      strB.toString
    }
  }

  def bindForDeleteState(
      stmt: Statement,
      slice: Int,
      entityType: String,
      persistenceId: String,
      revision: Long): _root_.io.r2dbc.spi.Statement

  def bindForUpsertState(
      stmt: Statement,
      getAndIncIndex: () => Int,
      slice: Int,
      entityType: String,
      state: DurableStateDao.SerializedStateRow,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings]): _root_.io.r2dbc.spi.Statement

}
