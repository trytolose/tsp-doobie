import com.dimafeng.testcontainers.PostgreSQLContainer
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import doobie.util.Write
import fs2.Stream
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.{Task, _}


object Main extends App {
  val tableValues: List[List[String]] = List(List("id", "int"), List("name", "varchar"))
  val tableName: String = "Users"
  val tableColumns: List[String] = List("id", "name")
  val millionUsers: List[User] = (1 to 1000000).toList.map(User(_, "Vasya"))

  type AppEnvironment = Clock with Database

  type AppTask[A] = TaskR[AppEnvironment, A]



//  val program: ZIO[Console, Throwable, Unit] = for {
//    container   <- ZIO(PostgreSQLContainer())
//    _           <- IO.effectTotal(container.start())
//    xa          =  DatabaseInterface.getTransactor(container)
//    dbInterface =  DatabaseInterface(xa)
//    _           <- dbInterface.createTable(tableName, tableValues)
//    _           <- dbInterface.insertMany(tableName, tableColumns, millionUsers)
//
//    queue       <- dbInterface.getQueue(50)
//    queueData   <- queue.takeAll
//    _           <- putStrLn(queueData.mkString)
//
//
//  } yield()

  val program = for {
      container   <- ZIO.effect(PostgreSQLContainer())
      _           <- ZIO.effect(container.start())

      blockingEC  <- blocking.blockingExecutor.map(_.asEC).provide(Blocking.Live)

      transactorR: Managed[Throwable, HikariTransactor[Task]] = Database.mkTransactor(
        container,
        Platform.executor.asEC,
        blockingEC
      )
      server = ZIO.runtime[AppEnvironment].flatMap { implicit rts =>
        db.createTable *> db.create(User(13, "usr")) *> db.get(13).flatMap(user => ZIO.effect(println(user)))
      }
      program <- transactorR.use { transactor =>
        server.provideSome[Environment] { _ =>
          new Clock.Live with Database.Live {
            override protected def tnx: doobie.Transactor[Task] = transactor
          }
        }
      }
  } yield program



  override def run(args: List[String]): ZIO[Main.Environment, Nothing, Int] = {
    program.fold(_ => 1, _ => 0)
  }

}


case class DatabaseInterface(tnx: Transactor[Task]) {
  import zio.interop.catz._

  def createTable(tableName: String, tableValues: List[List[String]]): Task[Unit] = {
    val unpackedTableValues: List[String] = tableValues.map(value => value.mkString(" "))
    val formattedTableValues: String = unpackedTableValues.mkString(",")
    val statement = fr"CREATE TABLE IF NOT EXISTS" ++ Fragment.const(tableName) ++ fr"(" ++ Fragment.const(formattedTableValues) ++ fr")"

    statement.update.run.transact(tnx).foldM(err => Task.fail(err), _ => Task.succeed(()))
  }

  def get(id: Int): Task[User] = {
    sql"""SELECT * FROM USERS WHERE ID = $id""".query[User].option.transact(tnx)
      .foldM(Task.fail, maybeUser => Task.require(UserNotFound(id))(Task.succeed(maybeUser)))
  }

  def getAll(): Task[List[User]] = {
    sql"""SELECT * FROM USERS""".query[User].to[List].transact(tnx)
      .foldM(err => Task.fail(err), list => Task.succeed(list))
  }

  def getAllStream(): Stream[Task, User] = {
    sql"""SELECT * FROM USERS""".query[User].stream.transact(tnx)
  }

  def getQueue(queueCapacity: Int): Task[Queue[User]] = {
    for {
      queue <- Queue.bounded[User](queueCapacity)
      q     <- getAllStream().evalMap(user => queue.offer(user).fork.asInstanceOf[Task[Boolean]]).compile.drain
    } yield queue
  }


  def insertOne(user: User): Task[User] = {
    sql"""INSERT INTO USERS (ID, NAME) VALUES (${user.id}, ${user.name})""".update.run
      .transact(tnx).foldM(err => Task.fail(err), _ => Task.succeed(user))
  }

  /** batch update, https://tpolecat.github.io/doobie/docs/07-Updating.html */
  def insertMany[T: Write](tableName: String, columns: List[String], values: List[T]):  Task[List[T]] = {
    import cats.implicits._

    val formattedColNames: String = columns.mkString(",")
    val numValues: String = columns.map(column => "?").mkString(", ")
    val statement = s"INSERT INTO $tableName ($formattedColNames) values($numValues)"

    Update[T](statement).updateMany(values).transact(tnx).foldM(err => Task.fail(err), _ => Task.succeed(values))
  }

}
object DatabaseInterface {
  import zio.interop.catz._

  def apply(tnx: Transactor[Task]): DatabaseInterface = new DatabaseInterface(tnx)

  def getTransactor(container: PostgreSQLContainer): Transactor[Task] = Transactor.fromDriverManager[Task](
    container.driverClassName,
    container.jdbcUrl,
    container.username,
    container.password,
  )
}






