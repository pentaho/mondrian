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

import mondrian.olap.DimensionType;
import mondrian.olap.Util;
import org.olap4j.OlapException;
import org.olap4j.impl.*;
import org.olap4j.metadata.*;

import java.util.Locale;

/**
 * Implementation of {@link org.olap4j.metadata.Dimension}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 24, 2007
 */
class MondrianOlap4jDimension implements Dimension, Named {
    private final MondrianOlap4jSchema olap4jSchema;
    private final mondrian.olap.Dimension dimension;

    MondrianOlap4jDimension(
        MondrianOlap4jSchema olap4jSchema,
        mondrian.olap.Dimension dimension)
    {
        this.olap4jSchema = olap4jSchema;
        this.dimension = dimension;
    }

    public boolean equals(Object obj) {
        return obj instanceof MondrianOlap4jDimension &&
            dimension.equals(((MondrianOlap4jDimension) obj).dimension);
    }

    public int hashCode() {
        return dimension.hashCode();
    }

    public NamedList<Hierarchy> getHierarchies() {
        final NamedList<MondrianOlap4jHierarchy> list =
            new NamedListImpl<MondrianOlap4jHierarchy>();
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        for (mondrian.olap.Hierarchy hierarchy : dimension.getHierarchies()) {
            list.add(olap4jConnection.toOlap4j(hierarchy));
        }
        return Olap4jUtil.cast(list);
    }

    public Hierarchy getDefaultHierarchy() {
        return getHierarchies().get(0);
    }

    public Type getDimensionType() throws OlapException {
        final DimensionType dimensionType = dimension.getDimensionType();
        switch (dimensionType) {
        case StandardDimension:
            return Type.OTHER;
        case MeasuresDimension:
            return Type.MEASURE;
        case TimeDimension:
            return Type.TIME;
        default:
            throw Util.unexpected(dimensionType);
        }
    }

    public String getName() {
        return dimension.getName();
    }

    public String getUniqueName() {
        return dimension.getUniqueName();
    }

    public String getCaption(Locale locale) {
        // TODO: locale caption
        return dimension.getCaption();
    }

    public String getDescription(Locale locale) {
        // TODO: locale description
        return dimension.getDescription();
    }
}

// End MondrianOlap4jDimension.java
