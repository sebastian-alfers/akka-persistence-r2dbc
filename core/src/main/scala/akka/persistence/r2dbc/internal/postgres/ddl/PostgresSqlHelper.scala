/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.postgres.ddl

import akka.persistence.r2dbc.internal.DurableStateDao
import akka.persistence.r2dbc.internal.postgres.ddl.PostgresSqlHelper.EvaluatedAdditionalColumnBindings
import akka.persistence.r2dbc.state.scaladsl.AdditionalColumn
import io.r2dbc.spi.Statement

object PostgresSqlHelper {
  final case class EvaluatedAdditionalColumnBindings(
      additionalColumn: AdditionalColumn[_, _],
      binding: AdditionalColumn.Binding[_])
}

trait PostgresSqlHelper {
  // is copied for now
  def bindTags(stmt: Statement, i: Int, state: DurableStateDao.SerializedStateRow): Statement = {
    if (state.tags.isEmpty)
      stmt.bindNull(i, classOf[Array[String]])
    else
      stmt.bind(i, state.tags.toArray)
  }

}
