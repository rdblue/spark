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

public class Transforms {
  private Transforms() {
  }

  public Transform apply(String name, Expression[] args) {
    return LogicalExpressions.apply(name, args);
  }

  public Transform bucket(int numBuckets, String[] columns) {
    return LogicalExpressions.bucket(numBuckets, columns);
  }

  public Transform identity(String column) {
    return LogicalExpressions.identity(column);
  }

  public Transform year(String column) {
    return LogicalExpressions.year(column);
  }

  public Transform month(String column) {
    return LogicalExpressions.month(column);
  }

  public Transform date(String column) {
    return LogicalExpressions.date(column);
  }

  public Transform dateHour(String column) {
    return LogicalExpressions.dateHour(column);
  }

}
