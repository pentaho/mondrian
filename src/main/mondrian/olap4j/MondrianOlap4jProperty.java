/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2010 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.impl.Named;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Property;

import java.util.Set;

/**
 * Implementation of {@link org.olap4j.metadata.Property}
 * for the Mondrian OLAP engine,
 * as a wrapper around a mondrian
 * {@link mondrian.olap.Property}.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 12, 2007
 */
class MondrianOlap4jProperty implements Property, Named {
    final mondrian.olap.Property property;

    MondrianOlap4jProperty(mondrian.olap.Property property) {
        this.property = property;
    }

    public Datatype getDatatype() {
        switch (property.getType()) {
        case TYPE_BOOLEAN:
            return Datatype.BOOLEAN;
        case TYPE_NUMERIC:
            return Datatype.UNSIGNED_INTEGER;
        case TYPE_STRING:
            return Datatype.STRING;
        case TYPE_OTHER:
            return Datatype.VARIANT;
        default:
            throw new RuntimeException("unexpected: " + property.getType());
        }
    }

    public Set<TypeFlag> getType() {
        return property.isCellProperty()
            ? TypeFlag.CELL_TYPE_FLAG
            : TypeFlag.MEMBER_TYPE_FLAG;
    }

    public String getName() {
        return property.name;
    }

    public String getUniqueName() {
        return property.name;
    }

    public String getCaption() {
        // todo: i18n
        return property.getCaption();
    }

    public String getDescription() {
        // todo: i18n
        return property.getDescription();
    }

    public boolean isVisible() {
        return !property.isInternal();
    }

    public ContentType getContentType() {
        return ContentType.REGULAR;
    }
}

// End MondrianOlap4jProperty.java
