/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.data

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampType, UserDefinedType}

private[spark] object InternalData {
  def getAccessor(dt: DataType): (SpecializedGetters, Int) => Any = dt match {
    case BooleanType => (input, ordinal) => input.getBoolean(ordinal)
    case ByteType => (input, ordinal) => input.getByte(ordinal)
    case ShortType => (input, ordinal) => input.getShort(ordinal)
    case IntegerType | DateType => (input, ordinal) => input.getInt(ordinal)
    case LongType | TimestampType => (input, ordinal) => input.getLong(ordinal)
    case FloatType => (input, ordinal) => input.getFloat(ordinal)
    case DoubleType => (input, ordinal) => input.getDouble(ordinal)
    case StringType => (input, ordinal) => input.getUTF8String(ordinal)
    case BinaryType => (input, ordinal) => input.getBinary(ordinal)
    case CalendarIntervalType => (input, ordinal) => input.getInterval(ordinal)
    case t: DecimalType => (input, ordinal) => input.getDecimal(ordinal, t.precision, t.scale)
    case t: StructType => (input, ordinal) => input.getStruct(ordinal, t.size)
    case _: ArrayType => (input, ordinal) => input.getArray(ordinal)
    case _: MapType => (input, ordinal) => input.getMap(ordinal)
    case u: UserDefinedType[_] => getAccessor(u.sqlType)
    case _ => (input, ordinal) => input.get(ordinal, dt)
  }

  def row(values: Any*): InternalRow = {
    org.apache.spark.sql.catalyst.data.InternalRow.of(values)
  }

  object Implicits {
    implicit class ScalaArrayData(array: ArrayData) {
      def foreach(elementType: DataType, f: (Int, Any) => Unit): Unit = {
        val size = array.numElements()
        val accessor = getAccessor(elementType)
        (0 until size).foreach { index =>
          if (array.isNullAt(index)) {
            f(index, null)
          } else {
            f(index, accessor(array, index))
          }
        }
      }

      def toArray[T: ClassTag](elementType: DataType): Array[T] = {
        val size = array.numElements()
        val accessor = getAccessor(elementType)
        val values = new Array[T](size)

        (0 until size).foreach { index =>
          if (array.isNullAt(index)) {
            values(index) = null.asInstanceOf[T]
          } else {
            values(index) = accessor(array, index).asInstanceOf[T]
          }
        }

        values
      }
    }

    implicit class ScalaMapData(map: MapData) {
      def foreach(keyType: DataType, valueType: DataType, f: (Any, Any) => Unit): Unit = {
        val size = map.numElements()
        val keys = map.keyArray()
        val values = map.valueArray()
        val keyAccessor = getAccessor(keyType)
        val valueAccessor = getAccessor(valueType)
        (0 until size).foreach { index =>
          f(keyAccessor(keys, index), valueAccessor(values, index))
        }
      }
    }
  }
}

