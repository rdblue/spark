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
