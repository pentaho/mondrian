/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.metadata.Property;
import org.olap4j.metadata.Datatype;
import org.olap4j.impl.Named;

import java.util.*;

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
    private final mondrian.olap.Property property;

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
        return TypeFlag.forMask(
            property.isCellProperty()
                ? TypeFlag.CELL.xmlaOrdinal
                : TypeFlag.MEMBER.xmlaOrdinal);
    }

    public String getName() {
        return property.name;
    }

    public String getUniqueName() {
        return property.name;
    }

    public String getCaption(Locale locale) {
        // todo: i18n
        return property.getCaption();
    }

    public String getDescription(Locale locale) {
        // todo: i18n
        return property.getDescription();
    }

    public ContentType getContentType() {
        return ContentType.REGULAR;
    }
}

// End MondrianOlap4jProperty.java
