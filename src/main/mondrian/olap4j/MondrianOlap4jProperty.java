/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap4j;

import org.olap4j.impl.Named;
import org.olap4j.metadata.Datatype;
import org.olap4j.metadata.Property;

import java.util.*;

/**
 * Implementation of {@link org.olap4j.metadata.Property}
 * for the Mondrian OLAP engine,
 * as a wrapper around a mondrian
 * {@link mondrian.olap.Property}.
 *
 * @author jhyde
 * @since Nov 12, 2007
 */
class MondrianOlap4jProperty implements Property, Named {
    /**
     * Map of member properties that are built into Mondrian but are not in the
     * olap4j standard.
     */
    static final Map<String, MondrianOlap4jProperty> MEMBER_EXTENSIONS =
        new LinkedHashMap<String, MondrianOlap4jProperty>();

    /**
     * Map of cell properties that are built into Mondrian but are not in the
     * olap4j standard.
     */
    static final Map<String, MondrianOlap4jProperty> CELL_EXTENSIONS =
        new LinkedHashMap<String, MondrianOlap4jProperty>();

    static {
        // Build set of names of olap4j standard member properties.
        final Set<String> memberNames = new HashSet<String>();
        for (Property property : Property.StandardMemberProperty.values()) {
            memberNames.add(property.getName());
        }

        final Set<String> cellNames = new HashSet<String>();
        for (Property property : StandardCellProperty.values()) {
            cellNames.add(property.getName());
        }

        for (mondrian.olap.Property o
            : mondrian.olap.Property.enumeration.getValuesSortedByName())
        {
            if (o.isMemberProperty()
                && !memberNames.contains(o.getName()))
            {
                MEMBER_EXTENSIONS.put(
                    o.getName(),
                    new MondrianOlap4jProperty(o));
            }
            if (o.isCellProperty()
                && !cellNames.contains(o.getName()))
            {
                CELL_EXTENSIONS.put(
                    o.getName(),
                    new MondrianOlap4jProperty(o));
            }
        }
    }

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
