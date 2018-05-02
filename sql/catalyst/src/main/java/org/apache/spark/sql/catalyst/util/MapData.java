package org.apache.spark.sql.catalyst.util;

import java.io.Serializable;

/**
 * Represents a map in Spark SQL that holds data values in Spark's internal representation.
 * <p>
 * For information on the internal data representation, see
 * {@link org.apache.spark.sql.catalyst.InternalRow}.
 * <p>
 * Concrete classes should not override {@code equals} or {@code hashCode} because the type cannot
 * be used as join keys, grouping keys, or in equality tests. See SPARK-9415 and PR#13847 for more
 * background.
 */
public interface MapData extends Serializable {

  /**
   * Return the number of key-value pairs in this map.
   * <p>
   * The key and value arrays returned by {@link #keyArray()} and {@link #valueArray()} both
   * contain this number of elements.
   *
   * @return the number of key-value pairs.
   */
  int numElements();

  /**
   * Return the map keys as {@link ArrayData}.
   * <p>
   * The key array returned by this method is aligned with the values array returned by
   * {@link #valueArray()}: for the key at index i, its corresponding value is at index i.
   *
   * @return the map keys
   */
  ArrayData keyArray();

  /**
   * Return the map values as {@link ArrayData}.
   * <p>
   * The value array returned by this method is aligned with the key array returned by
   * {@link #keyArray()}: the value value at index i corresponds to the key at index i.
   *
   * @return the map values
   */
  ArrayData valueArray();

  /**
   * Deep copy this map.
   *
   * @return a recursive copy of this map.
   */
  MapData copy();

}
