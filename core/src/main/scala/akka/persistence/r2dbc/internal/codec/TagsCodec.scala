/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.internal.codec

import java.nio.charset.StandardCharsets.UTF_8
import akka.annotation.InternalApi
import com.typesafe.config.Config
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

/**
 * INTERNAL API
 */
@InternalApi private[akka] sealed trait TagsCodec {
  def tagsClass: Class[_]
  def bindTags(statement: Statement, index: Int, tags: Set[String]): Statement
  def bindTags(statement: Statement, name: String, tags: Set[String]): Statement
  def getTags(row: Row, column: String): Array[String]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TagsCodec {
  case object PostgresTagsCodec extends TagsCodec {
    override def tagsClass: Class[Array[String]] = classOf[Array[String]]

    override def bindTags(statement: Statement, index: Int, tags: Set[String]): Statement =
      statement.bind(index, tags.toArray)

    override def bindTags(statement: Statement, name: String, tags: Set[String]): Statement =
      statement.bind(name, tags.toArray)

    def getTags(row: Row, column: String): Array[String] = row.get(column, classOf[Array[String]])
  }
  class SqlServerTagsCodec(connectionFactoryConfig: Config) extends TagsCodec {

    private val tagSeparator = connectionFactoryConfig.getString("tag-separator")

    require(tagSeparator.length == 1, s"Tag separator '$tagSeparator' must be a single character.")

    override def tagsClass: Class[String] = classOf[String]
    override def bindTags(statement: Statement, index: Int, tags: Set[String]): Statement =
      statement.bind(index, tags.mkString(","))

    override def bindTags(statement: Statement, name: String, tags: Set[String]): Statement =
      statement.bind(name, tags.mkString(","))

    override def getTags(row: Row, column: String): Array[String] = row.get(column, classOf[String]) match {
      case null    => Array.empty[String]
      case entries => entries.split(tagSeparator)
    }
  }
  implicit class RichTagsCodecStatement(val statement: Statement)(implicit codec: TagsCodec) extends AnyRef {
    def bindTagsNull(index: Int): Statement = statement.bindNull(index, codec.tagsClass)
    def bindTagsNull(index: String): Statement = statement.bindNull(index, codec.tagsClass)
    def bindTags(index: Int, tags: Set[String]): Statement = codec.bindTags(statement, index, tags)
    def bindTags(name: String, tags: Set[String]): Statement = codec.bindTags(statement, name, tags)
  }

  implicit class TagsRichRow(val row: Row)(implicit codec: TagsCodec) extends AnyRef {
    def getTags(column: String): Array[String] = codec.getTags(row, column)
  }
}
