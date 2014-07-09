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
import mondrian.rolap.RolapMeasure;

import org.olap4j.OlapException;
import org.olap4j.impl.AbstractNamedList;
import org.olap4j.impl.Named;
import org.olap4j.mdx.ParseTreeNode;
import org.olap4j.metadata.*;

import java.util.*;

/**
 * Implementation of {@link Member}
 * for the Mondrian OLAP engine,
 * as a wrapper around a mondrian
 * {@link mondrian.olap.Member}.
 *
 * @author jhyde
 * @since May 25, 2007
 */
class MondrianOlap4jMember
    extends MondrianOlap4jMetadataElement
    implements Member, Named
{
    private static final
        Map<Property.StandardCellProperty, mondrian.olap.Property>
        STANDARD_CELL_PROPERTY_MAP =
        initasd(Property.StandardCellProperty.values());

    private static final
        Map<Property.StandardMemberProperty, mondrian.olap.Property>
        STANDARD_MEMBER_PROPERTY_MAP =
        initasd(Property.StandardMemberProperty.values());

    private static <T extends Enum<T>> Map<T, mondrian.olap.Property> initasd(
        T[] values)
    {
        final List<String> missingProperties = Arrays.asList(
            "IS_DATAMEMBER",
            "IS_PLACEHOLDERMEMBER",
            "UPDATEABLE");
        final Map<T, mondrian.olap.Property> map =
            new HashMap<T, mondrian.olap.Property>();
        for (T p : values) {
            final mondrian.olap.Property value =
                mondrian.olap.Property.enumeration.getValue(p.name(), false);
            if (value == null) {
                assert missingProperties.contains(p.name()) : p.name();
            } else {
                map.put(p, value);
            }
        }
        return map;
    }

    final mondrian.olap.Member member;
    final MondrianOlap4jSchema olap4jSchema;

    MondrianOlap4jMember(
        MondrianOlap4jSchema olap4jSchema,
        mondrian.olap.Member mondrianMember)
    {
        assert mondrianMember != null;
        assert mondrianMember instanceof RolapMeasure
            == this instanceof MondrianOlap4jMeasure;
        this.olap4jSchema = olap4jSchema;
        this.member = mondrianMember;
    }

    public boolean equals(Object obj) {
        return obj instanceof MondrianOlap4jMember
            && member.equals(((MondrianOlap4jMember) obj).member);
    }

    public int hashCode() {
        return member.hashCode();
    }

    public String toString() {
        return getUniqueName();
    }

    public NamedList<MondrianOlap4jMember> getChildMembers()
        throws OlapException
    {
        final List<mondrian.olap.Member> children =
            olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData.olap4jConnection
                .getMondrianConnection().getSchemaReader()
                .withLocus().getMemberChildren(
                    member);
        return new AbstractNamedList<MondrianOlap4jMember>() {
            public String getName(Object member) {
                return ((MondrianOlap4jMember)member).getName();
            }

            public MondrianOlap4jMember get(int index) {
                return new MondrianOlap4jMember(
                    olap4jSchema, children.get(index));
            }

            public int size() {
                return children.size();
            }
        };
    }

    public int getChildMemberCount() throws OlapException {
        return olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
            .olap4jConnection.getMondrianConnection().getSchemaReader()
            .withLocus()
            .getMemberChildren(member).size();
    }

    public MondrianOlap4jMember getParentMember() {
        final mondrian.olap.Member parentMember = member.getParentMember();
        if (parentMember == null
            || !olap4jSchema.olap4jCatalog.olap4jDatabaseMetaData
            .olap4jConnection.getMondrianConnection2().getSchemaReader()
            .withLocus().isVisible(
                parentMember))
        {
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
        return getMemberType() == Type.FORMULA;
    }

    public int getSolveOrder() {
        return member.getSolveOrder();
    }

    public ParseTreeNode getExpression() {
        throw new UnsupportedOperationException();
    }

    public List<Member> getAncestorMembers() {
        final List<Member> list = new ArrayList<Member>();
        MondrianOlap4jMember m = getParentMember();
        while (m != null) {
            list.add(m);
            m = m.getParentMember();
        }
        return list;
    }

    public boolean isCalculatedInQuery() {
        return member.isCalculatedInQuery();
    }

    public Object getPropertyValue(Property property) {
        return member.getPropertyValue(asd(property));
    }

    private mondrian.olap.Property asd(Property property) {
        if (property instanceof MondrianOlap4jProperty) {
            return ((MondrianOlap4jProperty) property).property;
        } else if (property instanceof Property.StandardCellProperty) {
            return STANDARD_CELL_PROPERTY_MAP.get(property);
        } else if (property instanceof Property.StandardMemberProperty) {
            return STANDARD_MEMBER_PROPERTY_MAP.get(property);
        } else {
            return null;
        }
    }

    public String getPropertyFormattedValue(Property property) {
        mondrian.olap.Property prop = asd(property);
        return prop == null ? null
            : member.getPropertyFormattedValue(prop);
    }

    public void setProperty(Property property, Object value)
        throws OlapException
    {
        member.setProperty(asd(property), value);
    }

    public NamedList<Property> getProperties() {
        return getLevel().getProperties();
    }

    public int getOrdinal() {
        final Number ordinal =
            (Number) member.getPropertyValue(
                mondrian.olap.Property.MEMBER_ORDINAL);
        return ordinal.intValue();
    }

    public boolean isHidden() {
        return member.isHidden();
    }

    public int getDepth() {
        return member.getDepth();
    }

    public Member getDataMember() {
        final mondrian.olap.Member dataMember = member.getDataMember();
        if (dataMember == null) {
            return null;
        }
        return new MondrianOlap4jMember(olap4jSchema, dataMember);
    }

    public String getName() {
        return member.getName();
    }

    public String getUniqueName() {
        return member.getUniqueName();
    }

    public String getCaption() {
        return member.getLocalized(
            LocalizedProperty.CAPTION,
            olap4jSchema.getLocale());
    }

    public String getDescription() {
        return member.getLocalized(
            LocalizedProperty.DESCRIPTION,
            olap4jSchema.getLocale());
    }

    public boolean isVisible() {
        return (Boolean) member.getPropertyValue(
            mondrian.olap.Property.VISIBLE);
    }

    protected OlapElement getOlapElement() {
        return member;
    }
}

// End MondrianOlap4jMember.java
