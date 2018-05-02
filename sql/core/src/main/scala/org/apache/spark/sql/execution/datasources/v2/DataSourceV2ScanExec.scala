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

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.data.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.{ColumnarBatchScan, LeafExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.sources.v2.DataSourceV2
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Physical plan node for scanning data from a data source.
 */
case class DataSourceV2ScanExec(
    output: Seq[AttributeReference],
    @transient source: DataSourceV2,
    @transient options: Map[String, String],
    @transient reader: DataSourceReader)
  extends LeafExecNode with DataSourceV2StringFormat with ColumnarBatchScan {

  override def simpleString: String = "ScanV2 " + metadataString

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: DataSourceV2ScanExec =>
      output == other.output && reader.getClass == other.reader.getClass && options == other.options
    case _ => false
  }

  override def hashCode(): Int = {
    Seq(output, source, options).hashCode()
  }

  override def outputPartitioning: physical.Partitioning = reader match {
    case r: SupportsScanColumnarBatch if r.enableBatchRead() && batchReaderFactories.size == 1 =>
      SinglePartition

    case r: SupportsScanColumnarBatch if !r.enableBatchRead() && readerFactories.size == 1 =>
      SinglePartition

    case r if !r.isInstanceOf[SupportsScanColumnarBatch] && readerFactories.size == 1 =>
      SinglePartition

    case s: SupportsReportPartitioning =>
      new DataSourcePartitioning(
        s.outputPartitioning(), AttributeMap(output.map(a => a -> a.name)))

    case _ => super.outputPartitioning
  }

  private lazy val readerFactories: Seq[ReadTask[InternalRow]] = reader match {
    case r: SupportsDeprecatedScanRow =>
      r.createDataReaderFactories().asScala.map {
        new RowToUnsafeRowDataReaderFactory(_, reader.readSchema())
      }
    case _ =>
      reader.createReadTasks().asScala
  }

  private lazy val batchReaderFactories: Seq[ReadTask[ColumnarBatch]] = reader match {
    case r: SupportsScanColumnarBatch if r.enableBatchRead() =>
      assert(!reader.isInstanceOf[ContinuousReader],
        "continuous stream reader does not support columnar read yet.")
      r.createBatchReadTasks().asScala
  }

  private lazy val inputRDD: RDD[InternalRow] = reader match {
    case _: ContinuousReader =>
      EpochCoordinatorRef.get(
          sparkContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
          sparkContext.env)
        .askSync[Unit](SetReaderPartitions(readerFactories.size))
      new ContinuousDataSourceRDD(sparkContext, sqlContext, readerFactories)

    case r: SupportsScanColumnarBatch if r.enableBatchRead() =>
      // TODO: is this actually an instance of RDD[InternalRow]?
      new DataSourceRDD(sparkContext, batchReaderFactories).asInstanceOf[RDD[InternalRow]]

    case _ =>
      new DataSourceRDD(sparkContext, readerFactories)
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override val supportsBatch: Boolean = reader match {
    case r: SupportsScanColumnarBatch if r.enableBatchRead() => true
    case _ => false
  }

  override protected def needsUnsafeRowConversion: Boolean = false

  override protected def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      val numOutputRows = longMetric("numOutputRows")
      inputRDD.map { r =>
        numOutputRows += 1
        r
      }
    }
  }
}

class RowToUnsafeRowDataReaderFactory(rowReaderFactory: ReadTask[Row], schema: StructType)
  extends ReadTask[InternalRow] {

  override def preferredLocations: Array[String] = rowReaderFactory.preferredLocations

  override def createDataReader: DataReader[InternalRow] = {
    new RowToUnsafeDataReader(
      rowReaderFactory.createDataReader, RowEncoder.apply(schema).resolveAndBind())
  }
}

class RowToUnsafeDataReader(val rowReader: DataReader[Row], encoder: ExpressionEncoder[Row])
  extends DataReader[InternalRow] {

  override def next: Boolean = rowReader.next

  override def get: InternalRow = encoder.toRow(rowReader.get)

  override def close(): Unit = rowReader.close()
}
