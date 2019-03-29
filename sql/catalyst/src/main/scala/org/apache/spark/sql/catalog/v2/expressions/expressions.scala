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

package org.apache.spark.sql.catalog.v2.expressions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

/**
 * Helper methods for working with the logical expressions API.
 *
 * Factory methods can be used when referencing the logical expression nodes is ambiguous because
 * logical and internal expressions are used.
 */
private[sql] object LogicalExpressions {
  // a generic parser that is only used for parsing multi-part field names.
  // because this is only used for field names, the SQL conf passed in does not matter.
  private lazy val parser = new CatalystSqlParser(SQLConf.get)

  def fromPartitionColumns(columns: String*): Array[IdentityTransform] =
    columns.map(identity).toArray

  def fromBucketSpec(spec: BucketSpec): BucketTransform = {
    if (spec.sortColumnNames.nonEmpty) {
      throw new AnalysisException(
        s"Cannot convert bucketing with sort columns to a transform: $spec")
    }

    bucket(spec.numBuckets, spec.bucketColumnNames: _*)
  }

  implicit class TransformHelper(transforms: Seq[Transform]) {
    def asPartitionColumns: Seq[String] = {
      val (idTransforms, nonIdTransforms) = transforms.partition(_.isInstanceOf[IdentityTransform])

      if (nonIdTransforms.nonEmpty) {
        throw new AnalysisException("Transforms cannot be converted to partition columns: " +
            nonIdTransforms.map(_.describe).mkString(", "))
      }

      idTransforms.map(_.asInstanceOf[IdentityTransform]).map(_.reference).map { ref =>
        val parts = ref.fieldNames
        if (parts.size > 1) {
          throw new AnalysisException(s"Cannot partition by nested column: $ref")
        } else {
          parts(0)
        }
      }
    }
  }

  def literal[T](value: T): LiteralValue[T] = {
    val internalLit = catalyst.expressions.Literal(value)
    literal(value, internalLit.dataType)
  }

  def literal[T](value: T, dataType: DataType): LiteralValue[T] = LiteralValue(value, dataType)

  def reference(name: String): NamedReference =
    FieldReference(parser.parseMultipartIdentifier(name))

  def apply(name: String, arguments: Array[Expression]): Transform = ApplyTransform(name, arguments)

  def apply(name: String, arguments: Expression*): Transform = ApplyTransform(name, arguments)

  def bucket(numBuckets: Int, columns: Array[String]): BucketTransform =
    BucketTransform(literal(numBuckets, IntegerType), columns.map(reference))

  def bucket(numBuckets: Int, columns: String*): BucketTransform =
    BucketTransform(literal(numBuckets, IntegerType), columns.map(reference))

  def identity(column: String): IdentityTransform = IdentityTransform(reference(column))

  def year(column: String): YearTransform = YearTransform(reference(column))

  def month(column: String): MonthTransform = MonthTransform(reference(column))

  def date(column: String): DateTransform = DateTransform(reference(column))

  def dateHour(column: String): DateHourTransform = DateHourTransform(reference(column))
}

/**
 * Base class for simple transforms of a single column.
 */
private[sql] abstract class SingleColumnTransform(ref: NamedReference) extends Transform {

  def reference: NamedReference = ref

  override lazy val references: Array[NamedReference] = Array(ref)

  override lazy val arguments: Array[Expression] = Array(ref)

  override lazy val describe: String = name + "(" + reference.describe + ")"

  override def toString: String = describe
}

private[sql] final case class BucketTransform(
    numBuckets: Literal[Int],
    columns: Seq[NamedReference]) extends Transform {

  override lazy val name: String = "bucket"

  override lazy val references: Array[NamedReference] = {
    arguments
        .filter(_.isInstanceOf[NamedReference])
        .map(_.asInstanceOf[NamedReference])
  }

  override lazy val arguments: Array[Expression] = numBuckets +: columns.toArray

  override lazy val describe: String = s"bucket(${arguments.map(_.describe).mkString(", ")})"

  override def toString: String = describe
}

private[sql] final case class ApplyTransform(
    name: String,
    args: Seq[Expression]) extends Transform {

  override lazy val arguments: Array[Expression] = args.toArray

  override lazy val references: Array[NamedReference] = {
    arguments
        .filter(_.isInstanceOf[NamedReference])
        .map(_.asInstanceOf[NamedReference])
  }

  override lazy val describe: String = s"$name(${arguments.map(_.describe).mkString(", ")})"

  override def toString: String = describe
}

private[sql] final case class IdentityTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override lazy val name: String = "identity"
  override lazy val describe: String = ref.describe
}

private[sql] final case class YearTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override lazy val name: String = "year"
}

private[sql] final case class MonthTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override lazy val name: String = "month"
}

private[sql] final case class DateTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override lazy val name: String = "date"
}

private[sql] final case class DateHourTransform(
    ref: NamedReference) extends SingleColumnTransform(ref) {
  override lazy val name: String = "date_hour"
}

private[sql] final case class LiteralValue[T](value: T, dataType: DataType) extends Literal[T] {
  override def describe: String = {
    if (dataType.isInstanceOf[StringType]) {
      s"'$value'"
    } else {
      s"$value"
    }
  }
  override def toString: String = describe
}

private[sql] final case class FieldReference(parts: Seq[String]) extends NamedReference {
  override lazy val fieldNames: Array[String] = parts.toArray
  override lazy val describe: String = fieldNames.map(quote).mkString(".")
  override def toString: String = describe

  private def quote(part: String): String = {
    if (part.contains(".")) {
      s"`${part.replace("`", "``")}`"
    } else {
      part
    }
  }
}

private[sql] object FieldReference {
  def apply(column: String): NamedReference = {
    LogicalExpressions.reference(column)
  }
}
