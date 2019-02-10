package foo

import org.apache.spark.sql.Row
import cats.implicits._
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}

import scala.reflect._
import scala.reflect.runtime.universe._

trait AutoRow[T] {
  //implicit val ttag: TypeTag[T] = typeTag[T]
  //implicit val ctag: ClassTag[T] = classTag[T]

  //val myttag = implicitly[TypeTag[T]]
  implicit class ProductRow(p: Product) {
    def toRow(): Row = {
      Row(p.productIterator.map {
        case Some(value) => value
        case None => null
        case value => value
      }.toSeq: _*)
    }
  }

//  def toRow()(implicit tag: ClassTag[T]) = {
//    val fields = tag.runtimeClass.getDeclaredFields.map(f => {
//      f.setAccessible(true)
//      f
//    })
//    t: T => {
//      Row(fields.map {
//        _.get(t) match {
//          case Some(value) => value
//          case None => null
//          case value => value
//        }
//      }: _*)
//    }
//  }

  def fromRow(r: Row)(implicit ctag: ClassTag[T], ttag: TypeTag[T]): T = {
    val rm = runtimeMirror(classTag[T].runtimeClass.getClassLoader)
    val classTest = typeOf[T].typeSymbol.asClass
    val classMirror = rm.reflectClass(classTest)
    val constructor = typeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val constructorMirror = classMirror.reflectConstructor(constructor)

    val signatures = constructor.paramLists.flatten.map(_.typeSignature)
    val rowValues = r.toSeq.zip(signatures).map {
      case (value, signature) =>
        if (signature <:< typeOf[Option[_]]) {
          Option(value)
        } else {
          value
        }
    }
    constructorMirror(rowValues:_*).asInstanceOf[T]
  }

  def structType()(implicit ctag: ClassTag[T], ttag: TypeTag[T]): StructType = {
    val constructor = typeOf[T].decl(termNames.CONSTRUCTOR).asMethod
    val structFields = constructor.paramLists.flatten.map((param: Symbol) => {
      val dt = typeToStructType(param.typeSignature)
      StructField(param.name.toString, dt._1, nullable = dt._2)
    })
    StructType(structFields)
  }

  // scalastyle:off
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
