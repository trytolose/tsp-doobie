
case class UserNotFound(id: Int) extends Exception

case class User(id: Long, name: String)

trait HasQueries[T] {

  def createTableSql(): String

  def insertSql(): String

  def getSql(): String
}


object User {
  implicit def userQuerires = new HasQueries[User] {

    override def createTableSql(): String = ""

    override def insertSql(): String = ???

    override def getSql(): String = ???


  }
}


