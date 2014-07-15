/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2010 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapSchema;

import java.util.List;
import java.util.Map;

/**
 * Strategy to locate schemas and catalogs. Allows different
 * {@link mondrian.olap.MondrianServer servers} to do things differently.
 *
 * @author jhyde
 * @since 2010/11/12
 */
public interface CatalogFinder {
    /**
     * Returns a list of catalogs.
     *
     * <p>The catalog names occur in the natural order of the repository.
     *
     * @param connection Connection to mondrian
     * we want the catalog children.
     * @return List of catalogs
     */
    List<String> getCatalogNames(
        RolapConnection connection);

    /**
     * Returns a list of (schema name, schema) pairs in a catalog of a
     * particular name.
     *
     * <p>The name of the schema may not be the same as the value returned by
     * {@link mondrian.rolap.RolapSchema#getName()}. In fact, a given schema
     * may occur multiple times in the same catalog with different names.
     *
     * <p>The schemas occur in the natural order of the repository.
     *
     * @param connection Connection to mondrian
     * @param catalogName Name of catalog
     * @return List of catalogs
     */
    Map<String, RolapSchema> getRolapSchemas(
        RolapConnection connection,
        String catalogName);
}

// End CatalogFinder.java
