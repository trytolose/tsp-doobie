import com.dimafeng.testcontainers.PostgreSQLContainer
import zio.{Managed, Reservation, Task, TaskR, ZIO}

import scala.concurrent.ExecutionContext
import doobie.hikari.{HikariTransactor, _}
import doobie.implicits._
import doobie.{Query0, Transactor, Update0}
import zio.interop.catz.taskConcurrentInstances

trait Database extends Serializable {
  val userPersistence: Database.Service[Any]
}

object Database {

  trait Service[R] {
    val createTable: TaskR[R, Unit]
    def get(id: Int): TaskR[R, User]
    def create(user: User): TaskR[R, User]
    def delete(id: Int): TaskR[R, Unit]
  }

  def mkTransactor(
                    cont: PostgreSQLContainer,
                    connectEC: ExecutionContext,
                    transactEC: ExecutionContext
                  ): Managed[Throwable, HikariTransactor[Task]] = {
    import zio.interop.catz._

    val xa = HikariTransactor
      .newHikariTransactor[Task](cont.driverClassName, cont.jdbcUrl, cont.username, cont.password, connectEC, transactEC)

    val res = xa.allocated.map {
      case (transactor, cleanupM) =>
        Reservation(ZIO.succeed(transactor), cleanupM.orDie)
    }.uninterruptible

    Managed(res)
  }

  trait Live extends Database {

    protected def tnx: Transactor[Task]

    val userPersistence: Service[Any] = new Service[Any] {

      val createTable: Task[Unit] =
        SQL.createTable.run.transact(tnx).foldM(err => Task.fail(err), _ => Task.succeed(()))

      def get(id: Int): Task[User] =
        SQL
          .get(id)
          .option
          .transact(tnx)
          .foldM(
            Task.fail,
            maybeUser => Task.require(UserNotFound(id))(Task.succeed(maybeUser))
          )

      def create[T: HadQueries](t: T): Task[User] = {
        val sql: String = implicitly[HadQueries[T]].insertSql()
          SQL
            .create(user)
            .run
            .transact(tnx)
            .foldM(err => Task.fail(err), _ => Task.succeed(user))

      }

      def delete(id: Int): Task[Unit] =
        SQL
          .delete(id)
          .run
          .transact(tnx)
          .unit
          .orDie
    }

    object SQL {

      def createTable: Update0 = sql"""CREATE TABLE IF NOT EXISTS Users (id int PRIMARY KEY, name varchar)""".update

      def get(id: Int): Query0[User] =
        sql"""SELECT * FROM USERS WHERE ID = $id """.query[User]

      def create(user: User): Update0 =
        sql"""INSERT INTO USERS (ID, NAME) VALUES (${user.id}, ${user.name})""".update

      def delete(id: Int): Update0 =
        sql"""DELETE FROM USERS WHERE ID = $id""".update
    }

  }


}

package object db extends Database.Service[Database] {
  val createTable: TaskR[Database, Unit]        = ZIO.accessM(_.userPersistence.createTable)
  def get(id: Int): TaskR[Database, User]       = ZIO.accessM(_.userPersistence.get(id))
  def create(user: User): TaskR[Database, User] = ZIO.accessM(_.userPersistence.create(user))
  def delete(id: Int): TaskR[Database, Unit]    = ZIO.accessM(_.userPersistence.delete(id))
}


