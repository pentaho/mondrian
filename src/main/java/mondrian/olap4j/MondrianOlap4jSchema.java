/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.olap4j;

import mondrian.olap.LocalizedProperty;
import mondrian.olap.OlapElement;
import mondrian.olap.Role;

import mondrian.rolap.*;

import org.olap4j.OlapException;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;

import java.util.*;

/**
 * Implementation of {@link org.olap4j.metadata.Schema}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @since May 24, 2007
 */
class MondrianOlap4jSchema
    extends MondrianOlap4jMetadataElement
    implements Schema, Named
{
    final MondrianOlap4jCatalog olap4jCatalog;
    final String schemaName;
    final RolapSchema schema;

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
        RolapSchema schema)
    {
        this.olap4jCatalog = olap4jCatalog;
        this.schemaName = schemaName;
        this.schema = schema;
    }

    public Catalog getCatalog() {
        return olap4jCatalog;
    }

    public String getUniqueName() {
        return schema.getUniqueName();
    }

    public String getCaption() {
        return schema.getLocalized(
            LocalizedProperty.CAPTION,
            getLocale());
    }

    public String getDescription() {
        return schema.getLocalized(
            LocalizedProperty.DESCRIPTION,
            getLocale());
    }

    public boolean isVisible() {
        return schema.isVisible();
    }

    public NamedList<Cube> getCubes() throws OlapException {
        NamedList<MondrianOlap4jCube> list =
            new NamedListImpl<MondrianOlap4jCube>();
        final MondrianOlap4jConnection olap4jConnection =
            olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        for (mondrian.olap.Cube cube
            : olap4jConnection.getMondrianConnection()
                .getSchemaReader().getCubes())
        {
            // Hide the dummy cube created for each shared dimension.
            if (cube.getName().startsWith("$")) {
                continue;
            }
            list.add(olap4jConnection.toOlap4j(cube));
        }
        return Olap4jUtil.cast(list);
    }

    public NamedList<Dimension> getSharedDimensions() throws OlapException {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        final SortedSet<MondrianOlap4jDimension> dimensions =
            new TreeSet<MondrianOlap4jDimension>(
                new Comparator<MondrianOlap4jDimension>() {
                    public int compare(
                        MondrianOlap4jDimension o1,
                        MondrianOlap4jDimension o2)
                    {
                        return o1.getName().compareTo(o2.getName());
                    }
                }
            );
        final Role role = olap4jConnection.getMondrianConnection().getRole();
        for (RolapCubeDimension dim : schema.getSharedDimensionList()) {
            if (role.canAccess(dim)) {
                dimensions.add(
                    olap4jConnection.toOlap4j(dim));
            }
        }
        NamedList<MondrianOlap4jDimension> list =
            new NamedListImpl<MondrianOlap4jDimension>();
        list.addAll(dimensions);
        return Olap4jUtil.cast(list);
    }

    public Collection<Locale> getSupportedLocales() {
        return schema.locales;
    }

    public String getName() {
        return schemaName;
    }

    /**
     * Shorthand for catalog.database.connection.getLocale().
     * Not part of the olap4j api; do not make public.
     *
     * @return Locale of current connection
     */
    final Locale getLocale() {
        return olap4jCatalog.olap4jDatabase.getOlapConnection().getLocale();
    }

    protected OlapElement getOlapElement() {
        return schema;
    }
}

// End MondrianOlap4jSchema.java
