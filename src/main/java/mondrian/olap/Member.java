/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import java.util.List;

/**
 * A <code>Member</code> is a 'point' on a dimension of a cube. Examples are
 * <code>[Time].[1997].[January]</code>,
 * <code>[Customer].[All Customers]</code>,
 * <code>[Customer].[USA].[CA]</code>,
 * <code>[Measures].[Unit Sales]</code>.
 *
 * <p> Every member belongs to a {@link Level} of a {@link Hierarchy}. Members
 * except the root member have a parent, and members not at the leaf level
 * have one or more children.
 *
 * <p> Measures are a special kind of member. They belong to their own
 * dimension, <code>[Measures]</code>.
 *
 * <p> There are also special members representing the 'All' value of a
 * hierarchy, the null value, and the error value.
 *
 * <p> Members can have member properties. Their {@link Level#getProperties}
 * defines which are allowed.
 *
 * @author jhyde, 2 March, 1999
 */
public interface Member extends OlapElement, Comparable, Annotated {

    /**
     * Returns this member's parent, or null (not the 'null member', as
     * returned by {@link Hierarchy#getNullMember}) if it has no parent.
     *
     * <p>In an access-control context, a member may have no <em>visible</em>
     * parents, so use {@link SchemaReader#getMemberParent}.
     */
    Member getParentMember();

    Level getLevel();

    Hierarchy getHierarchy();

    /**
     * Returns name of parent member, or empty string (not null) if we are the
     * root.
     */
    String getParentUniqueName();

    /**
     * Returns the type of member.
     */
    MemberType getMemberType();

    boolean isParentChildLeaf();

    enum MemberType {
        UNKNOWN,
        REGULAR, // adMemberRegular
        ALL,
        MEASURE,
        FORMULA,
        /**
         * This member is its hierarchy's NULL member (such as is returned by
         * <code>[Gender]&#46;[All Gender]&#46;PrevMember</code>, for example).
         */
        NULL
    }

    /**
     * Only allowable if the member is part of the <code>WITH</code> clause of
     * a query.
     */
    void setName(String name);

    /** Returns whether this is the 'all' member. */
    boolean isAll();

    /** Returns whether this is a member of the measures dimension. */
    boolean isMeasure();

    /** Returns whether this is the 'null member'. */
    boolean isNull();

    /**
     * Returns whether <code>member</code> is equal to, a child, or a
     * descendent of this <code>Member</code>.
     */
    boolean isChildOrEqualTo(Member member);

    /** Returns whether this member is computed using either a <code>with
     * member</code> clause in an mdx query or a calculated member defined in
     * cube. */
    boolean isCalculated();

    /**
     * Returns whether this member should be evaluated within the Evaluator.
     *
     * <p>Normally {@link #isCalculated} and {@link #isEvaluated} should return
     * the same value, but in situations where mondrian would like to treat the
     * two concepts separately such in role based security, these values may
     * differ.
     *
     * @return true if evaluated
     */
    boolean isEvaluated();
    int getSolveOrder();
    Exp getExpression();

    /**
     * Returns a list of the ancestor members of this member.
     *
     * @deprecated Use
     *     {@link SchemaReader#getMemberAncestors(Member, java.util.List)}.
     */
    List<Member> getAncestorMembers();

    /**
     * Returns whether this member is computed from a {@code WITH MEMBER}
     * clause in an MDX query.
     */
    boolean isCalculatedInQuery();

    /**
     * Returns the value of the given property.
     */
    Object getPropertyValue(Property property);

    /**
     * Returns the value of the property named <code>propertyName</code>.
     * Name match is case-sensitive.
     *
     * @deprecated Use {@link #getPropertyValue(Property)}
     */
    Object getPropertyValue(String propertyName);

    /**
     * Returns the value of the property named <code>propertyName</code>,
     * matching according to the required case-sensitivity.
     *
     * @deprecated Use {@link #getPropertyValue(Property)}
     */
    Object getPropertyValue(String propertyName, boolean matchCase);

    /**
     * Returns the formatted value of the given property.
     */
    String getPropertyFormattedValue(Property property);

    /**
     * Returns the formatted value of the property named
     * <code>propertyName</code>.
     *
     * @deprecated Use {@link #getPropertyFormattedValue(Property)}
     */
    String getPropertyFormattedValue(String propertyName);

    /**
     * Sets a property of this member to a given value.
     */
    void setProperty(Property property, Object value);

    /**
     * Sets a property of this member to a given value.
     *
     * @deprecated Use {@link #setProperty(Property, Object)}
     */
    void setProperty(String propertyName, Object value);

    /**
     * Returns the definitions of the properties this member may have.
     */
    Property[] getProperties();

    /**
     * Returns the ordinal of the member.
     * @deprecated Use {@link #getOrderKey()}.
     */
    int getOrdinal();

    /**
     * Returns the order key of the member (relative to its siblings);
     * null if undefined or unavailable.
     */
    Comparable getOrderKey();

    /**
     * Returns whether this member is 'hidden', as per the rules which define
     * a ragged hierarchy.
     */
    boolean isHidden();

    /**
     * returns the depth of this member, which is not the level's depth
     *  in case of parent child dimensions
     * @return depth
     */
    int getDepth();

    /**
     * Returns the system-generated data member that is associated with a
     * nonleaf member of a dimension.
     *
     * <p>Returns this member if this member is a leaf member, or if the
     * nonleaf member does not have an associated data member.</p>
     */
    Member getDataMember();
}

// End Member.java
