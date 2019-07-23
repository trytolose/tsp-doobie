import com.dimafeng.testcontainers.PostgreSQLContainer
import zio.{Managed, Queue, Reservation, Task, TaskR, ZIO}

import scala.concurrent.ExecutionContext
import doobie.hikari.{HikariTransactor, _}
import doobie.implicits._
import doobie.util.update.Update
import doobie.{Query0, Transactor, Update0}
import zio.interop.catz.taskConcurrentInstances

trait Database extends Serializable {
  val userPersistence: Database.Service[Any]
}

object Database {

  trait Service[R] {
    def createTable[T: HadQueries](tableName: String): TaskR[R, Unit]
    def get[T: HadQueries](id: Int): TaskR[R, T]
    def create[T: HadQueries](t: T): TaskR[R, T]
    def getQueue[T: HadQueries](): TaskR[R, Queue[T]]
    def updateBatch[T: HadQueries](list: List[T]): TaskR[R, List[T]]

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
      def createTable[T: HadQueries](tableName: String): TaskR[Any, Unit] = {
        implicitly[HadQueries[T]]
      }

      override def get[T: HadQueries](id: Int): TaskR[Any, T] = ???

      override def create[T: HadQueries](t: T): TaskR[Any, T] = {
            val sql: String = implicitly[HadQueries[T]].insertSql()
            sql"""$sql""".update.run.transact(tnx).foldM(err => Task.fail(err), _ => Task.succeed(t))
      }

      override def getQueue[T: HadQueries](): TaskR[Any, Queue[T]] = ???

      override def updateBatch[T: HadQueries](list: List[T]): TaskR[Any, List[T]] = ???
    }



  }
//  def create[T: HadQueries](t: T): Task[T] = {
//    val sql: String = implicitly[HadQueries[T]].insertSql()
//    sql"""$sql""".update.run.transact(tnx).foldM(err => Task.fail(err), _ => Task.succeed(t))
//
//  }


}

package object db extends Database.Service[Database] {
  val createTable: TaskR[Database, Unit]        = ZIO.accessM(_.userPersistence.createTable)
  def get(id: Int): TaskR[Database, User]       = ZIO.accessM(_.userPersistence.get(id))
  def create(user: User): TaskR[Database, User] = ZIO.accessM(_.userPersistence.create(user))
  def delete(id: Int): TaskR[Database, Unit]    = ZIO.accessM(_.userPersistence.delete(id))
}


