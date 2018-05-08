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

package org.apache.spark.sql.execution.datasources.v2

import scala.reflect.ClassTag

import org.codehaus.commons.compiler.CompileException
import org.codehaus.janino.InternalCompilerException

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratePredicate, Predicate}
import org.apache.spark.sql.sources.v2.reader.InputPartition

class DataSourceRDDPartition[T <: InternalRow](
    val index: Int, val inputPartition: InputPartition[T])
  extends Partition with Serializable

class DataSourceRDD[T <: InternalRow: ClassTag](
    sc: SparkContext,
    inputSchema: Seq[Attribute],
    @transient private val inputPartitions: Seq[InputPartition[T]])
  extends RDD[T](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    inputPartitions.zipWithIndex.map {
      case (inputPartition, index) => new DataSourceRDDPartition(index, inputPartition)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val inputPartition = split.asInstanceOf[DataSourceRDDPartition[T]].inputPartition
    val reader = inputPartition.createPartitionReader()
    context.addTaskCompletionListener(_ => reader.close())
    val iter = new Iterator[T] {
      private[this] var valuePrepared = false

      override def hasNext: Boolean = {
        if (!valuePrepared) {
          valuePrepared = reader.next()
        }
        valuePrepared
      }

      override def next(): T = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        valuePrepared = false
        reader.get()
      }
    }

    val residual = inputPartition.residualFilters.reduceLeftOption(And)
    val predicate = residual.map(newPredicate(_, inputSchema))

    val filtered = predicate.map(pred => iter.filter(row => pred.eval(row))).getOrElse(iter)

    new InterruptibleIterator(context, filtered)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition[T]].inputPartition.preferredLocations()
  }

  protected def newPredicate(
      expression: Expression, inputSchema: Seq[Attribute]): Predicate = {
    try {
      GeneratePredicate.generate(expression, inputSchema)
    } catch {
      case _ @ (_: InternalCompilerException | _: CompileException) =>
        val logMessage = expression.toString match {
          case str if str.length > 256 => str.substring(0, 256 - 3) + "..."
          case str => str
        }
        logWarning(s"Falling back to interpreted, codegen failed for expression: $logMessage")
        InterpretedPredicate.create(expression, inputSchema)
    }
  }
}
