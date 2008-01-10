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
import org.olap4j.OlapException;
import org.olap4j.impl.Named;
import org.olap4j.impl.AbstractNamedList;
import org.olap4j.mdx.ParseTreeNode;

import java.util.List;
import java.util.Locale;

/**
 * Implementation of {@link Member}
 * for the Mondrian OLAP engine,
 * as a wrapper around a mondrian
 * {@link mondrian.olap.Member}.
 *
 * @author jhyde
 * @version $Id$
 * @since May 25, 2007
 */
class MondrianOlap4jMember implements Member, Named {
    final mondrian.olap.Member member;
    private final MondrianOlap4jSchema olap4jSchema;

    MondrianOlap4jMember(
        MondrianOlap4jSchema olap4jSchema,
        mondrian.olap.Member mondrianMember)
    {
        assert mondrianMember != null;
        this.olap4jSchema = olap4jSchema;
        this.member = mondrianMember;
    }

    public boolean equals(Object obj) {
        return obj instanceof MondrianOlap4jMember &&
            member.equals(((MondrianOlap4jMember) obj).member);
    }

    public int hashCode() {
        return member.hashCode();
    }

    public NamedList<MondrianOlap4jMember> getChildMembers() {
        final mondrian.olap.Member[] children =
            olap4jSchema.schemaReader.getMemberChildren(
                member);
        return new AbstractNamedList<MondrianOlap4jMember>() {
            protected String getName(MondrianOlap4jMember member) {
                return member.getName();
            }

            public MondrianOlap4jMember get(int index) {
                return new MondrianOlap4jMember(olap4jSchema, children[index]);
            }

            public int size() {
                return children.length;
            }
        };
    }

    public int getChildMemberCount() {
        return olap4jSchema.schemaReader.getMemberChildren(member).length;
    }

    public MondrianOlap4jMember getParentMember() {
        final mondrian.olap.Member parentMember = member.getParentMember();
        if (parentMember == null) {
            return null;
        }
        return new MondrianOlap4jMember(olap4jSchema, parentMember);
    }

    public Level getLevel() {
        return new MondrianOlap4jLevel(olap4jSchema, member.getLevel());
    }

    public Hierarchy getHierarchy() {
        return new MondrianOlap4jHierarchy(
            olap4jSchema, member.getHierarchy());
    }

    public Dimension getDimension() {
        return new MondrianOlap4jDimension(
            olap4jSchema, member.getDimension());
    }

    public Type getMemberType() {
        return Type.valueOf(member.getMemberType().name());
    }

    public boolean isAll() {
        return member.isAll();
    }

    public boolean isChildOrEqualTo(Member member) {
        throw new UnsupportedOperationException();
    }

    public boolean isCalculated() {
        throw new UnsupportedOperationException();
    }

    public int getSolveOrder() {
        throw new UnsupportedOperationException();
    }

    public ParseTreeNode getExpression() {
        throw new UnsupportedOperationException();
    }

    public List<Member> getAncestorMembers() {
        throw new UnsupportedOperationException();
    }

    public boolean isCalculatedInQuery() {
        throw new UnsupportedOperationException();
    }

    public Object getPropertyValue(Property property) {
        return member.getPropertyValue(property.getName());
    }

    public String getPropertyFormattedValue(Property property) {
        return member.getPropertyFormattedValue(property.getName());
    }

    public void setProperty(Property property, Object value) throws OlapException {
        member.setProperty(property.getName(), value);
    }

    public NamedList<Property> getProperties() {
        return getLevel().getProperties();
    }

    public int getOrdinal() {
        final Number ordinal =
            (Number) member.getPropertyValue(
                Property.StandardMemberProperty.MEMBER_ORDINAL.getName());
        return ordinal.intValue();
    }

    public boolean isHidden() {
        throw new UnsupportedOperationException();
    }

    public int getDepth() {
        throw new UnsupportedOperationException();
    }

    public Member getDataMember() {
        throw new UnsupportedOperationException();
    }

    public String getName() {
        return member.getName();
    }

    public String getUniqueName() {
        return member.getUniqueName();
    }

    public String getCaption(Locale locale) {
        throw new UnsupportedOperationException();
    }

    public String getDescription(Locale locale) {
        throw new UnsupportedOperationException();
    }
}

// End MondrianOlap4jMember.java
