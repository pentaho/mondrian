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
import org.olap4j.impl.*;

import java.util.Locale;

/**
 * Implementation of {@link org.olap4j.metadata.Hierarchy}
 * for the Mondrian OLAP engine.
 *
 * @author jhyde
 * @version $Id$
 * @since May 25, 2007
 */
class MondrianOlap4jHierarchy implements Hierarchy, Named {
    private final MondrianOlap4jSchema olap4jSchema;
    private final mondrian.olap.Hierarchy hierarchy;

    MondrianOlap4jHierarchy(
        MondrianOlap4jSchema olap4jSchema,
        mondrian.olap.Hierarchy hierarchy)
    {
        this.olap4jSchema = olap4jSchema;
        this.hierarchy = hierarchy;
    }

    public boolean equals(Object obj) {
        return obj instanceof MondrianOlap4jHierarchy &&
            hierarchy.equals(((MondrianOlap4jHierarchy) obj).hierarchy);
    }

    public int hashCode() {
        return hierarchy.hashCode();
    }

    public Dimension getDimension() {
        return new MondrianOlap4jDimension(
            olap4jSchema, hierarchy.getDimension());
    }

    public NamedList<Level> getLevels() {
        final NamedList<MondrianOlap4jLevel> list =
            new NamedListImpl<MondrianOlap4jLevel>();
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        for (mondrian.olap.Level level : hierarchy.getLevels()) {
            list.add(olap4jConnection.toOlap4j(level));
        }
        return Olap4jUtil.cast(list);
    }

    public boolean hasAll() {
        return hierarchy.hasAll();
    }

    public Member getDefaultMember() {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        return olap4jConnection.toOlap4j(hierarchy.getDefaultMember());
    }

    public NamedList<Member> getRootMembers() {
        final MondrianOlap4jConnection olap4jConnection =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection;
        final mondrian.olap.Member[] levelMembers =
            olap4jConnection.connection.getSchemaReader().getLevelMembers(
                hierarchy.getLevels()[0], false);
        return new AbstractNamedList<Member>() {
            protected String getName(Member member) {
                return member.getName();
            }

            public Member get(int index) {
                return olap4jConnection.toOlap4j(levelMembers[index]);
            }

            public int size() {
                return levelMembers.length;
            }
        };
    }

    public String getName() {
        return hierarchy.getName();
    }

    public String getUniqueName() {
        return hierarchy.getUniqueName();
    }

    public String getCaption(Locale locale) {
        // todo: localize caption
        return hierarchy.getCaption();
    }

    public String getDescription(Locale locale) {
        // todo: localize description
        return hierarchy.getDescription();
    }
}

// End MondrianOlap4jHierarchy.java
