/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.ddl

import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.postgres.ddl.PostgresSqlHelper.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import io.r2dbc.spi.Statement

trait BaseSqlHelper {
  def bindAdditionalColumns(
      getAndIncIndex: () => Int,
      stmt: Statement,
      additionalBindings: IndexedSeq[EvaluatedAdditionalColumnBindings]): Statement = {
    additionalBindings.foreach {
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.BindValue(v)) =>
        stmt.bind(getAndIncIndex(), v)
      case EvaluatedAdditionalColumnBindings(col, AdditionalColumn.BindNull) =>
        stmt.bindNull(getAndIncIndex(), col.fieldClass)
      case EvaluatedAdditionalColumnBindings(_, AdditionalColumn.Skip) =>
    }
    stmt
  }

}
