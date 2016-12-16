/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap4j;

import mondrian.olap.Access;
import mondrian.olap.OlapElement;
import mondrian.rolap.RolapSchema;

import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.OlapException;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;

import java.util.Map;

/**
 * Implementation of {@link Catalog}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @since May 23, 2007
 */
class MondrianOlap4jCatalog
    extends MondrianOlap4jMetadataElement
    implements Catalog, Named
{
    final MondrianOlap4jDatabaseMetaData olap4jDatabaseMetaData;
    final String name;
    final MondrianOlap4jDatabase olap4jDatabase;

    MondrianOlap4jCatalog(
        MondrianOlap4jDatabaseMetaData olap4jDatabaseMetaData,
        String name,
        MondrianOlap4jDatabase database)
    {
        assert database != null;
        this.olap4jDatabaseMetaData = olap4jDatabaseMetaData;
        this.name = name;
        this.olap4jDatabase = database;
    }

    public NamedList<Schema> getSchemas() throws OlapException {
        final NamedList<MondrianOlap4jSchema> list =
            new NamedListImpl<MondrianOlap4jSchema>();
        final MondrianOlap4jConnection oConn =
            ((MondrianOlap4jConnection)olap4jDatabase
                .getOlapConnection());
        NamedList<MondrianOlap4jSchema> schemas = oConn.getSchemas(this);
        for (MondrianOlap4jSchema olap4jSchema : schemas) {
            final mondrian.olap.Schema schema = olap4jSchema.schema;
            if (oConn
                .getMondrianConnection().getRole().getAccess(schema)
                != Access.NONE)
            {
                list.add(olap4jSchema);
            }
        }
        return Olap4jUtil.cast(list);
    }

    public String getName() {
        return name;
    }

    public OlapDatabaseMetaData getMetaData() {
        return olap4jDatabaseMetaData;
    }

    public Database getDatabase() {
        return olap4jDatabase;
    }

    protected OlapElement getOlapElement() {
        return null;
    }
}

// End MondrianOlap4jCatalog.java
