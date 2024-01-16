/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.r2dbc.state

import akka.Done
import akka.actor.testkit.typed.scaladsl.{ LogCapturing, ScalaTestWithActorTestKit }
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.TestActors.DurableStatePersister
import akka.persistence.r2dbc.{ TestActors, TestConfig, TestData, TestDbLifecycle }
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.typed.PersistenceId
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class ConcurrentInsertSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory
        .parseString("""
        akka.persistence.r2dbc {
          query.persistence-ids.buffer-size = 20
          state {
            custom-table {
              "CustomEntity" = durable_state_test
            }
          }
        }
        """)
        .withFallback(TestConfig.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {

  override def typedSystem: ActorSystem[_] = system
  implicit val ex = typedSystem.executionContext

  val customTable = "test_insert"

  private val createTable = if (r2dbcSettings.dialectName == "sqlserver") {
    s"IF object_id('$customTable') is null CREATE TABLE $customTable (pid varchar(256), rev bigint, the_value varchar(256))"
  } else {
    s"create table if not exists $customTable as select * from durable_state where persistence_id = ''"
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    Await.result(r2dbcExecutor.executeDdl("create table")(_.createStatement(createTable)), 10.seconds)

    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from $customTable")),
      10.seconds)

  }

  "Durable State persistenceIds" should {
    "retrieve all ids" in {

      val pids = 0 until 100
      val inserts: Seq[Future[Long]] = pids.map { pid =>
        r2dbcExecutor.updateOne(s"insert into $customTable")(sf => {
          sf.createStatement(s"insert into $customTable (pid, rev, the_value) VALUES(@pid, @rev, @theValue)")
            .bind("@pid", "abc")
            .bind("@rev", 123)
            .bind("@theValue", "def")
        })
      }

      Future.sequence(inserts).futureValue.size shouldBe (pids.size)
    }
  }
}
