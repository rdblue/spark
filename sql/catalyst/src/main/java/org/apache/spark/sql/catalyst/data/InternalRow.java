package org.apache.spark.sql.catalyst.data;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.catalyst.util.SpecializedSetters;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents a row in Spark SQL that holds data values in Spark's internal representation.
 * <p>
 * This class can be used to provide values directly to Spark that will not be translated. For
 * example, when accessing a timestamp from InternalRow, the row returns an unboxed long value that
 * encodes the timestamp as microseconds from the Unix epoch.
 * <p>
 * {@link InternalRow} must return values in the following representations, by data type:
 * <ul>
 * <li>{@link BooleanType} - unboxed boolean</li>
 * <li>{@link ByteType} - unboxed byte</li>
 * <li>{@link ShortType} - unboxed short</li>
 * <li>{@link IntegerType} - unboxed int</li>
 * <li>{@link LongType} - unboxed long</li>
 * <li>{@link FloatType} - unboxed float</li>
 * <li>{@link DoubleType} - unboxed double</li>
 * <li>{@link DecimalType} - {@link Decimal} that contains the value's scale, precision, and
 *     unscaled value. Unscaled value is stored as an unboxed long if precision is less than 19 or
 *     with a {@link BigDecimal} for precision 19 or greater</li>
 * <li>{@link DateType} - unboxed int that encodes the number of days from the Unix epoch,
 *     1970-01-01</li>
 * <li>{@link TimestampType} - unboxed long that encodes microseconds from the Unix epoch,
 *     1970-01-01T00:00:00.000000</li>
 * <li>{@link CalendarIntervalType} - {@link CalendarInterval}</li>
 * <li>{@link StringType} - {@link UTF8String} backed by a byte array storing UTF-8 encoded text
 * </li>
 * <li>{@link BinaryType} - byte[]</li>
 * <li>{@link ArrayType} - {@link ArrayData}</li>
 * <li>{@link MapType} - {@link MapData}</li>
 * <li>{@link StructType} - {@link InternalRow}</li>
 * <li>{@link NullType} - always null. Accessed by {@link #isNullAt(int)} and set by
 *     {@link #setNullAt(int)}, never using get or update.</li>
 * </ul>
 */
public interface InternalRow extends SpecializedGetters, SpecializedSetters {

  /**
   * Construct an {@link InternalRow} from an array of values.
   *
   * @param values an array of value objects
   * @return an InternalRow where the values are columns by ordinal
   */
  static InternalRow of(Object... values) {
    return new GenericInternalRow(values);
  }

  /**
   * Construct an {@link InternalRow} with the given number of fields.
   *
   * @param numFields the number of fields in the row
   * @return an InternalRow
   */
  static InternalRow create(int numFields) {
    return new GenericInternalRow(numFields);
  }

  /**
   * Construct an {@link InternalRow} from a {@link Seq} of values.
   *
   * @param values a Seq of value objects
   * @return an InternalRow where values from the seq are columns by ordinal
   */
  static InternalRow fromSeq(Seq<?> values) {
    return new GenericInternalRow(JavaConversions.asJavaCollection(values).toArray());
  }

  /**
   * Deep copies the given value if it is a string, struct, array, or map type.
   *
   * @param value a value to copy
   * @return a deep copy of the value
   */
  static Object copyValue(Object value) {
    if (value instanceof UTF8String) {
      return ((UTF8String) value).copy();
    } else if (value instanceof InternalRow) {
      return ((InternalRow) value).copy();
    } else if (value instanceof ArrayData) {
      return ((ArrayData) value).copy();
    } else if (value instanceof MapData) {
      return ((MapData) value).copy();
    } else {
      return value;
    }
  }

  /**
   * Returns an empty {@link InternalRow}.
   */
  InternalRow empty = of(); // interface fields are public static final by default

  /**
   * @return the number of fields in this row.
   */
  int numFields();

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
   *
   * @param fieldTypes the type of each field in the row
   * @return a Seq of the values from this row
   */
  default Seq<?> toSeq(Seq<DataType> fieldTypes) {
    int numFields = numFields();
    List<Object> array = new ArrayList<>(numFields);

    for (int i = 0; i < numFields; i += 1) {
      array.add(get(i, fieldTypes.apply(i)));
    }

    return JavaConversions.asScalaBuffer(array).toIndexedSeq();
  }

  /**
   * Return a Scala Seq representing the row. Elements are placed in the same order in the Seq.
   *
   * @param rowType the type of the row
   * @return a Seq of the values from this row
   */
  default Seq<?> toSeq(StructType rowType) {
    int numFields = numFields();
    List<Object> array = new ArrayList<>(numFields);

    for (int i = 0; i < numFields; i += 1) {
      array.add(get(i, rowType.apply(i).dataType()));
    }

    return JavaConversions.asScalaBuffer(array).toIndexedSeq();
  }

  /**
   * Updates the value of field {@code i}.
   * <p>
   * Note that after updating, the given value will be kept in this row, and the caller side should
   * guarantee that this value won't be changed afterwards.
   *
   * Note: In order to support update decimal with precision > 18 in UnsafeRow,
   * CAN NOT call setNullAt() for decimal column on UnsafeRow, call setDecimal(i, null, precision).
   *
   * @param ordinal index in the row to set
   * @param value value to set
   */
  default void setDecimal(int ordinal, Decimal value, int precision) {
    update(ordinal, value);
  }

  /**
   * Check whether any value is NULL.
   *
   * @return true if any value is NULL
   */
  default boolean anyNull() {
    int len = numFields();
    for (int i = 0; i < len; i += 1) {
      if (isNullAt(i)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Deep copy this row.
   *
   * @return a recursive copy of this row.
   */
  InternalRow copy();

}
