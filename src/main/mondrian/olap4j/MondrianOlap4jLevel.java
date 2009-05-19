/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2007-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap4j;

import org.olap4j.metadata.*;
import org.olap4j.impl.ArrayNamedListImpl;
import org.olap4j.impl.Named;

import java.util.*;

import mondrian.olap.Util;

/**
 * Implementation of {@link Level}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 25, 2007
 */
class MondrianOlap4jLevel implements Level, Named {
    private final MondrianOlap4jSchema olap4jSchema;
    private final mondrian.olap.Level level;

    /**
     * Creates a MondrianOlap4jLevel.
     *
     * @param olap4jSchema Schema
     * @param level Mondrian level
     */
    MondrianOlap4jLevel(
        MondrianOlap4jSchema olap4jSchema,
        mondrian.olap.Level level)
    {
        this.olap4jSchema = olap4jSchema;
        this.level = level;
    }

    public boolean equals(Object obj) {
        return obj instanceof MondrianOlap4jLevel &&
            level.equals(((MondrianOlap4jLevel) obj).level);
    }

    public int hashCode() {
        return level.hashCode();
    }

    public int getDepth() {
        return level.getDepth();
    }

    public Hierarchy getHierarchy() {
        return new MondrianOlap4jHierarchy(olap4jSchema, level.getHierarchy());
    }

    public Dimension getDimension() {
        return new MondrianOlap4jDimension(olap4jSchema, level.getDimension());
    }

    public boolean isCalculated() {
        return false;
    }

    public Type getLevelType() {
        switch (level.getLevelType()) {
        case Regular:
            return Type.REGULAR;
        case TimeDays:
            return Type.TIME_DAYS;
        case TimeHalfYear:
            return Type.TIME_HALF_YEAR;
        case TimeHours:
            return Type.TIME_HOURS;
        case TimeMinutes:
            return Type.TIME_MINUTES;
        case TimeMonths:
            return Type.TIME_MONTHS;
        case TimeQuarters:
            return Type.TIME_QUARTERS;
        case TimeSeconds:
            return Type.TIME_SECONDS;
        case TimeUndefined:
            return Type.TIME_UNDEFINED;
        case TimeWeeks:
            return Type.TIME_WEEKS;
        case TimeYears:
            return Type.TIME_YEARS;
        case Null:
        default:
            throw Util.unexpected(level.getLevelType());
        }
    }

    public NamedList<Property> getProperties() {
        final NamedList<Property> list = new ArrayNamedListImpl<Property>() {
            protected String getName(Property property) {
                return property.getName();
            }
        };
        // standard properties first
        list.addAll(
            Arrays.asList(Property.StandardMemberProperty.values()));
        // then level-specific properties
        for (mondrian.olap.Property property : level.getProperties()) {
            list.add(new MondrianOlap4jProperty(property));
        }
        return list;
    }

    public List<Member> getMembers() {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        final mondrian.olap.SchemaReader schemaReader =
            olap4jConnection.connection.getSchemaReader();
        final List<mondrian.olap.Member> levelMembers =
            schemaReader.getLevelMembers(level, true);
        return new AbstractList<Member>() {
            public Member get(int index) {
                return olap4jConnection.toOlap4j(levelMembers.get(index));
            }

            public int size() {
                return levelMembers.size();
            }
        };
    }

    public String getName() {
        return level.getName();
    }

    public String getUniqueName() {
        return level.getUniqueName();
    }

    public String getCaption(Locale locale) {
        // todo: localized captions
        return level.getCaption();
    }

    public String getDescription(Locale locale) {
        // todo: localize
        return level.getDescription();
    }

    public int getCardinality() {
        return level.getApproxRowCount();
    }
}

// End MondrianOlap4jLevel.java
