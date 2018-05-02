package org.apache.spark.sql.catalyst.util;

import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import scala.collection.IndexedSeq;

import java.io.Serializable;

/**
 * Represents a map in Spark SQL that holds data values in Spark's internal representation.
 * <p>
 * For information on the internal data representation, see
 * {@link org.apache.spark.sql.catalyst.InternalRow}.
 */
public interface ArrayData extends SpecializedGetters, SpecializedSetters, Serializable {
  /**
   * Return the number of elements in this array.
   *
   * @return the number of elements.
   */
  int numElements();

  /**
   * Deep copy this array.
   *
   * @return a recursive copy of this array.
   */
  ArrayData copy();

  default boolean[] toBooleanArray() {
    int size = numElements();
    boolean[] values = new boolean[size];
    for (int i = 0; i < size; i += 1) {
      values[i] = getBoolean(i);
    }

    return values;
  }

  default byte[] toByteArray() {
    int size = numElements();
    byte[] values = new byte[size];
    for (int i = 0; i < size; i += 1) {
      values[i] = getByte(i);
    }

    return values;
  }

  default short[] toShortArray() {
    int size = numElements();
    short[] values = new short[size];
    for (int i = 0; i < size; i += 1) {
      values[i] = getShort(i);
    }

    return values;
  }

  default int[] toIntArray() {
    int size = numElements();
    int[] values = new int[size];
    for (int i = 0; i < size; i += 1) {
      values[i] = getInt(i);
    }

    return values;
  }

  default long[] toLongArray() {
    int size = numElements();
    long[] values = new long[size];
    for (int i = 0; i < size; i += 1) {
      values[i] = getLong(i);
    }

    return values;
  }

  default float[] toFloatArray() {
    int size = numElements();
    float[] values = new float[size];
    for (int i = 0; i < size; i += 1) {
      values[i] = getFloat(i);
    }

    return values;
  }

  default double[] toDoubleArray() {
    int size = numElements();
    double[] values = new double[size];
    for (int i = 0; i < size; i += 1) {
      values[i] = getDouble(i);
    }

    return values;
  }

  default Object[] toObjectArray(DataType elementType) {
    int size = numElements();
    Object[] values = new Object[size];
    for (int i = 0; i < size; i += 1) {
      values[i] = get(i, elementType);
    }

    return values;
  }

  default <T> IndexedSeq<T> toSeq(DataType dataType) {
    return new ArrayDataIndexedSeq<>(this, dataType);
  }

  static ArrayData toArrayData(boolean[] input) {
    return UnsafeArrayData.fromPrimitiveArray(input);
  }

  static ArrayData toArrayData(byte[] input) {
    return UnsafeArrayData.fromPrimitiveArray(input);
  }

  static ArrayData toArrayData(short[] input) {
    return UnsafeArrayData.fromPrimitiveArray(input);
  }

  static ArrayData toArrayData(int[] input) {
    return UnsafeArrayData.fromPrimitiveArray(input);
  }

  static ArrayData toArrayData(long[] input) {
    return UnsafeArrayData.fromPrimitiveArray(input);
  }

  static ArrayData toArrayData(float[] input) {
    return UnsafeArrayData.fromPrimitiveArray(input);
  }

  static ArrayData toArrayData(double[] input) {
    return UnsafeArrayData.fromPrimitiveArray(input);
  }

  static ArrayData toArrayData(Object[] input) {
    return new GenericArrayData(input);
  }
}
