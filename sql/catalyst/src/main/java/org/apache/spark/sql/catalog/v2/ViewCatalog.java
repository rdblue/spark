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

import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException;

/**
 * Catalog methods for working with Views.
 */
public interface ViewCatalog extends CatalogPlugin {
  /**
   * List the views in a namespace from the catalog.
   * <p>
   * If the catalog supports tables, this must return identifiers for only views and not tables.
   *
   * @param namespace a multi-part namespace
   * @return an array of Identifiers for views
   * @throws NoSuchNamespaceException If the namespace does not exist (optional).
   */
  Identifier[] listViews(String[] namespace) throws NoSuchNamespaceException;

  /**
   * Load view metadata by {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports tables and contains a table for the identifier and not a view, this
   * must throw {@link NoSuchViewException}.
   *
   * @param ident a view identifier
   * @return the view's metadata
   * @throws NoSuchViewException If the view doesn't exist or is a table
   */
  View loadView(Identifier ident) throws NoSuchViewException;

  /**
   * Test whether a view exists using an {@link Identifier identifier} from the catalog.
   * <p>
   * If the catalog supports views and contains a view for the identifier and not a table, this
   * must return false.
   *
   * @param ident a view identifier
   * @return true if the view exists, false otherwise
   */
  default boolean viewExists(Identifier ident) {
    try {
      return loadView(ident) != null;
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  /**
   * Create a view in the catalog.
   *
   * @param ident a view identifier
   * @param sql SQL text that defines the view
   * @return metadata for the new view
   * @throws ViewAlreadyExistsException If a view or table already exists for the identifier
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  View createView(
      Identifier ident,
      String sql) throws ViewAlreadyExistsException, NoSuchNamespaceException;

  /**
   * Drop a view in the catalog.
   * <p>
   * If the catalog supports tables and contains a table for the identifier and not a view, this
   * must not drop the table and must return false.
   *
   * @param ident a view identifier
   * @return true if a view was deleted, false if no view exists for the identifier
   */
  boolean dropView(Identifier ident);
}
