/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
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

import mondrian.olap.*;
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
    private final mondrian.olap.Schema schema;
    final SchemaReader schemaReader;

    MondrianOlap4jSchema(
        MondrianOlap4jCatalog olap4jCatalog,
        SchemaReader schemaReader,
        mondrian.olap.Schema schema)
    {
        this.olap4jCatalog = olap4jCatalog;
        this.schemaReader = schemaReader;
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
        return schema.getName();
    }
}

// End MondrianOlap4jSchema.java
