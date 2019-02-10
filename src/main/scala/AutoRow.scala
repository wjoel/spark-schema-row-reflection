import org.apache.spark.sql.Row
import cats.implicits._
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.reflect._
import scala.reflect.runtime.universe._

abstract class AutoRow[T](implicit tt: TypeTag[T], ct: ClassTag[T]) {
  implicit class ProductRow(p: Product) {
    def toRow: Row = {
      Row(p.productIterator.map {
        case Some(value) => value
        case None => null
        case value => value
      }.toSeq: _*)
    }
  }

  private val rm = runtimeMirror(classTag[T].runtimeClass.getClassLoader)
  private val classTest = typeOf[T].typeSymbol.asClass
  private val classMirror = rm.reflectClass(classTest)
  private val constructor = typeOf[T].decl(termNames.CONSTRUCTOR).asMethod
  private val constructorMirror = classMirror.reflectConstructor(constructor)
  private val constructorSymbols = constructor.paramLists.flatten
  private val constructorParamIsOptional = constructorSymbols
    .map(_.typeSignature <:< typeOf[Option[_]])

  def fromRow(r: Row): T = {
    val rowValues = r.toSeq.zip(constructorParamIsOptional).map {
      case (value, true) => Option(value)
      case (value, false) => value
    }
    constructorMirror(rowValues:_*).asInstanceOf[T]
  }

  val structType: StructType = {
    val structFields = constructorSymbols.map((param: Symbol) => {
      val dt = typeToStructType(param.typeSignature)
      StructField(param.name.toString, dt._1, nullable = dt._2)
    })
    StructType(structFields)
  }

  private def typeToStructType(t: Type): (DataType, Boolean) = {
    val nullable = t <:< typeOf[Option[_]]
    val rt = if (nullable) {
      t.typeArgs.head // Fetch the type param of Option[_]
    } else {
      t
    }

    val dt = rt match {
      case x if x =:= typeOf[Byte] => DataTypes.ByteType
      case x if x =:= typeOf[Short] => DataTypes.ShortType
      case x if x =:= typeOf[Int] => DataTypes.IntegerType
      case x if x =:= typeOf[Long] => DataTypes.LongType
      case x if x =:= typeOf[Float] => DataTypes.FloatType
      case x if x =:= typeOf[Double] => DataTypes.DoubleType
      case x if x =:= typeOf[String] => DataTypes.StringType
      case x if x <:< typeOf[Map[_,_]] =>
        val valueType = typeToStructType(rt.typeArgs.get(1).get)
        DataTypes.createMapType(typeToStructType(rt.typeArgs.get(0).get)._1, valueType._1, valueType._2)
      case x if x <:< typeOf[Seq[_]] =>
        val valueType = typeToStructType(rt.typeArgs.head)
        DataTypes.createArrayType(valueType._1, valueType._2)
    }
    (dt, nullable)
  }
}
