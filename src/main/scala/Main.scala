package foo

case class AssetsItem(name: String,
                      parentId: Option[Long],
                      description: Option[String],
                      metadata: Option[Map[String, String]],
                      id: Long)

object AssetsItem extends AutoRow[AssetsItem]

object Main extends App {
  //val toRow = AssetsItem.toRow()
  val ai = AssetsItem("asdf", None, None, None, 123L)
  //val row = toRow(ai)
  //println(row.mkString(", "))
  println(ai.toRow().mkString(", "))
  println(AssetsItem.fromRow(ai.toRow()))
  println(AssetsItem.structType.toString())
}
