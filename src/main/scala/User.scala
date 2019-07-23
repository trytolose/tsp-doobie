
case class UserNotFound(id: Int) extends Exception

case class User(id: Long, name: String)

trait HadQueries[T] {

  def insertSql(): String

  def getSql(): String
}


object User {
  implicit def userQuerires = new HadQueries[User] {
    override def insertSql(): String = ???

    override def getSql(): String = ???
  }
}


