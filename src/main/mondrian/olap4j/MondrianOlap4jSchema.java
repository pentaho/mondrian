/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.metadata.*;
import org.olap4j.metadata.Cube;
import org.olap4j.metadata.Dimension;
import org.olap4j.metadata.Schema;
import org.olap4j.OlapException;
import org.olap4j.impl.*;

import java.util.Locale;
import java.util.Collection;
import java.util.Collections;

import mondrian.olap.Hierarchy;

/**
 * Implementation of {@link org.olap4j.metadata.Schema}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 24, 2007
 */
class MondrianOlap4jSchema implements Schema, Named {
    final MondrianOlap4jCatalog olap4jCatalog;
    final String schemaName;
    final mondrian.olap.Schema schema;

    /**
     * Creates a MondrianOlap4jSchema.
     *
     * <p>The name of the schema is not necessarily the same as
     * schema.getName(). If schema was loaded in a datasources.xml file, the
     * name it was given there (in the &lt;Catalog&gt; element) trumps the name
     * in the catalog.xml file.
     *
     * @param olap4jCatalog Catalog containing schema
     * @param schemaName Name of schema
     * @param schema Mondrian schema
     */
    MondrianOlap4jSchema(
        MondrianOlap4jCatalog olap4jCatalog,
        String schemaName,
        mondrian.olap.Schema schema)
    {
        this.olap4jCatalog = olap4jCatalog;
        this.schemaName = schemaName;
        this.schema = schema;
    }

    public Catalog getCatalog() {
        return olap4jCatalog;
    }

    public NamedList<Cube> getCubes() throws OlapException {
        NamedList<MondrianOlap4jCube> list =
            new NamedListImpl<MondrianOlap4jCube>();
        final MondrianOlap4jConnection olap4jConnection =
            olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        for (mondrian.olap.Cube cube : schema.getCubes()) {
            list.add(olap4jConnection.toOlap4j(cube));
        }
        return Olap4jUtil.cast(list);
    }

    public NamedList<Dimension> getSharedDimensions() throws OlapException {
        NamedList<MondrianOlap4jDimension> list =
            new NamedListImpl<MondrianOlap4jDimension>();
        final MondrianOlap4jConnection olap4jConnection =
            olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        for (Hierarchy hierarchy : schema.getSharedHierarchies()) {
            list.add(olap4jConnection.toOlap4j(hierarchy.getDimension()));
        }
        return Olap4jUtil.cast(list);
    }

    public Collection<Locale> getSupportedLocales() throws OlapException {
        return Collections.emptyList();
    }

    public String getName() {
        return schemaName;
    }
}

// End MondrianOlap4jSchema.java
