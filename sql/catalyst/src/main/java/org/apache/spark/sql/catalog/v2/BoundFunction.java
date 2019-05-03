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

package org.apache.spark.sql.catalog.v2;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;

/**
 * Represents a function that is bound to an input type.
 */
public interface BoundFunction extends Function {

  /**
   * Returns the {@link DataType data type} of values produced by this function.
   * <p>
   * For example, a "plus" function may return {@link IntegerType} when it is bound to arguments
   * that are also {@link IntegerType}.
   *
   * @return a data type for values produced by this function
   */
  DataType resultType();

}
