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

import org.olap4j.metadata.Catalog;
import org.olap4j.metadata.NamedList;
import org.olap4j.metadata.Schema;
import org.olap4j.OlapException;
import org.olap4j.OlapDatabaseMetaData;
import org.olap4j.impl.*;

/**
 * Implementation of {@link Catalog}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 23, 2007
 */
class MondrianOlap4jCatalog implements Catalog, Named {
    final MondrianOlap4jDatabaseMetaData olap4jDatabaseMetaData;

    MondrianOlap4jCatalog(
        MondrianOlap4jDatabaseMetaData olap4jDatabaseMetaData) {
        this.olap4jDatabaseMetaData = olap4jDatabaseMetaData;
    }

    public NamedList<Schema> getSchemas() throws OlapException {
        // A mondrian instance contains one schema, so implicitly it contains
        // one catalog
        NamedList<MondrianOlap4jSchema> list =
            new NamedListImpl<MondrianOlap4jSchema>();
        final mondrian.olap.Schema schema =
            olap4jDatabaseMetaData.olap4jConnection.connection.getSchema();
        list.add(
            olap4jDatabaseMetaData.olap4jConnection.toOlap4j(schema));
        return Olap4jUtil.cast(list);
    }

    public String getName() {
        return MondrianOlap4jConnection.LOCALDB_CATALOG_NAME;
    }

    public OlapDatabaseMetaData getMetaData() {
        return olap4jDatabaseMetaData;
    }
}

// End MondrianOlap4jCatalog.java
