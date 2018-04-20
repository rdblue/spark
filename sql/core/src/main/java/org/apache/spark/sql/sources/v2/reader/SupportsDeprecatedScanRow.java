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

package org.apache.spark.sql.sources.v2.reader;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.List;

/**
 * A mix-in interface for {@link DataSourceReader} to assist in moving from Row to InternalRow.
 * Data source readers can implement this interface to output {@link Row}.
 */
@Deprecated
@InterfaceStability.Evolving
public interface SupportsDeprecatedScanRow extends DataSourceReader {
  @Override
  default List<ReadTask<InternalRow>> createReadTasks() {
    throw new IllegalStateException(
        "createReadTasks not supported by default within SupportsDeprecatedScanRow.");
  }

  /**
   * Returns a list of reader factories. Each factory is responsible for creating a data reader to
   * output data for one RDD partition. That means the number of factories returned here is same as
   * the number of RDD partitions this scan outputs.
   *
   * Note that, this may not be a full scan if the data source reader mixes in other optimization
   * interfaces like column pruning, filter push-down, etc. These optimizations are applied before
   * Spark issues the scan request.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  List<ReadTask<Row>> createDataReaderFactories();
}
