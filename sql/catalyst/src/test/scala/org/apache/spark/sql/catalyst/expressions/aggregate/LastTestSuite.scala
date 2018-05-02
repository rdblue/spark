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
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.catalyst.data.InternalData
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Literal}
import org.apache.spark.sql.types.IntegerType

class LastTestSuite extends SparkFunSuite {
  val input = AttributeReference("input", IntegerType, nullable = true)()
  val evaluator = DeclarativeAggregateEvaluator(Last(input, Literal(false)), Seq(input))
  val evaluatorIgnoreNulls = DeclarativeAggregateEvaluator(Last(input, Literal(true)), Seq(input))

  test("empty buffer") {
    assert(evaluator.initialize() === InternalData.row(null, false))
  }

  test("update") {
    val result = evaluator.update(
      InternalData.row(1),
      InternalData.row(9),
      InternalData.row(-1))
    assert(result === InternalData.row(-1, true))
  }

  test("update - ignore nulls") {
    val result1 = evaluatorIgnoreNulls.update(
      InternalData.row(null),
      InternalData.row(9),
      InternalData.row(null))
    assert(result1 === InternalData.row(9, true))

    val result2 = evaluatorIgnoreNulls.update(
      InternalData.row(null),
      InternalData.row(null))
    assert(result2 === InternalData.row(null, false))
  }

  test("merge") {
    // Empty merge
    val p0 = evaluator.initialize()
    assert(evaluator.merge(p0) === InternalData.row(null, false))

    // Single merge
    val p1 = evaluator.update(InternalData.row(1), InternalData.row(-99))
    assert(evaluator.merge(p1) === p1)

    // Multiple merges.
    val p2 = evaluator.update(InternalData.row(2), InternalData.row(10))
    assert(evaluator.merge(p1, p2) === p2)

    // Empty partitions (p0 is empty)
    assert(evaluator.merge(p1, p0, p2) === p2)
    assert(evaluator.merge(p2, p1, p0) === p1)
  }

  test("merge - ignore nulls") {
    // Multi merges
    val p1 = evaluatorIgnoreNulls.update(InternalData.row(1), InternalData.row(null))
    val p2 = evaluatorIgnoreNulls.update(InternalData.row(null), InternalData.row(null))
    assert(evaluatorIgnoreNulls.merge(p1, p2) === p1)
  }

  test("eval") {
    // Null Eval
    assert(evaluator.eval(InternalData.row(null, true)) === InternalData.row(null))
    assert(evaluator.eval(InternalData.row(null, false)) === InternalData.row(null))

    // Empty Eval
    val p0 = evaluator.initialize()
    assert(evaluator.eval(p0) === InternalData.row(null))

    // Update - Eval
    val p1 = evaluator.update(InternalData.row(1), InternalData.row(-99))
    assert(evaluator.eval(p1) === InternalData.row(-99))

    // Update - Merge - Eval
    val p2 = evaluator.update(InternalData.row(2), InternalData.row(10))
    val m1 = evaluator.merge(p1, p0, p2)
    assert(evaluator.eval(m1) === InternalData.row(10))

    // Update - Merge - Eval (empty partition at the end)
    val m2 = evaluator.merge(p2, p1, p0)
    assert(evaluator.eval(m2) === InternalData.row(-99))
  }

  test("eval - ignore nulls") {
    // Update - Merge - Eval
    val p1 = evaluatorIgnoreNulls.update(InternalData.row(1), InternalData.row(null))
    val p2 = evaluatorIgnoreNulls.update(InternalData.row(null), InternalData.row(null))
    val m1 = evaluatorIgnoreNulls.merge(p1, p2)
    assert(evaluatorIgnoreNulls.eval(m1) === InternalData.row(1))
  }
}
