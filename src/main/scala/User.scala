import scala.collection.mutable.HashMap

case class User(id: String, followingList: HashMap[String, Int] = null)