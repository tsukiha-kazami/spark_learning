package join

/**
 * @author Shi Lei
 * @create 2020-12-07
 */
object Test {

  def main(args: Array[String]): Unit = {
    val map: Map[String, UserInfo] = Array(
      UserInfo("user_1", "name_1", "address_1"),
      UserInfo("user_2", "name_2", "address_2"),
      UserInfo("user_3", "name_3", "address_3"),
      UserInfo("user_4", "name_4", "address_4"),
      UserInfo("user_5", "name_5", "address_5")
    ).map(x => x.userID -> x).toMap

    println(map)
  }
}
