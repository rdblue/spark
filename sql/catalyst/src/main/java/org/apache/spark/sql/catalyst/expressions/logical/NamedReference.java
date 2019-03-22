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

package org.apache.spark.sql.catalyst.expressions.logical;

import org.apache.spark.annotation.Experimental;

/**
 * Represents a field or column reference in the public logical expression API.
 */
@Experimental
public interface NamedReference extends Expression {
  /**
   * Returns the referenced field name.
   * <p>
   * Names that identify nested fields are formatted using a "." separator without quoting or
   * additional escape characters. A struct column "a" with a field named "b.c" will produce the
   * field name "a.b.c".
   */
  String fieldName();
}
