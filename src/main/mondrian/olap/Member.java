/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 March, 1999
*/

package mondrian.olap;
import java.io.*;
import java.util.*;

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
 **/
public interface Member extends OlapElement {

	Member getParentMember();

	Level getLevel();

	Hierarchy getHierarchy();

	/**
	 * Returns name of parent member, or empty string (not null) if we are the
	 * root.
	 */
	String getParentUniqueName();

	String getCaption();

  	/**
  	 * Returns the type of member. Values are {@link #UNKNOWN_MEMBER_TYPE},
	 * {@link #REGULAR_MEMBER_TYPE}, {@link #ALL_MEMBER_TYPE}, {@link
	 * #MEASURE_MEMBER_TYPE}, {@link #FORMULA_MEMBER_TYPE}.
  	 **/
	int getMemberType();

	static final int UNKNOWN_MEMBER_TYPE = 0;
	static final int REGULAR_MEMBER_TYPE = 1;
	static final int ALL_MEMBER_TYPE = 2;
	static final int MEASURE_MEMBER_TYPE = 3;
	static final int FORMULA_MEMBER_TYPE = 4;
	/**
	 * This member is its hierarchy's NULL member (such as is returned by
	 * <code>[Gender].[All Gender].PrevMember</code>, for example)
	 */
	static final int NULL_MEMBER_TYPE = 5;

	static final String memberTypes[] = {
		"unknown", "regular", "all", "measure", "formula", "null"
	};

	/**
	 * Only allowable if the member is part of the <code>WITH</code> clause of
	 * a query.
	 **/
	void setName(String name);

	int getDepth();

	/** Returns whether this is the 'all' member. */
	boolean isAll();

	/** Returns whether this is a member of the measures dimension. */
	boolean isMeasure();

	/** Returns whether this is the 'null member'. **/
	boolean isNull();

	/**
	 * Returns a negative integer, zero, or a positive integer if this object
	 * comes earlier than <code>other</code> in a prefix traversal (that is, it
	 * is an ancestor or a earlier sibling of an ancestor, or a descendent of
	 * an earlier sibling), is a sibling, or comes later in a prefix
	 * traversal.
	 **/
	int compareHierarchically(Member other);

	/**
	 * Returns whether <code>member</code> is equal to, a child, or a
	 * descendent of this <code>Member</code>.
	 **/
	boolean isChildOrEqualTo(Member member);

	/** Returns whether this member is computed from a 'with member ...'
	 * formula in an mdx query OR it's calculated member defined in cube */
	boolean isCalculated();

	/**
	 * Returns a member <code>n</code> further along in the same level.
	 */
	Member getLeadMember(int n);

	/**
	 * Finds a child member in this member.
	 **/
	Member lookupChildMember(String s);

	/**
	 * Returns array of all members, which are ancestor to <code>this</code>.
	 **/
	Member[] getAncestorMembers();

	/**
	 * Returns whether this member is computed from a 'with member ...'
	 * formula in an mdx query.
	 **/
	boolean isCalculatedInQuery();

	/**
	 * Returns the expression which yields a format string for this member. If
	 * the format string is a constant, then the expression will be a string
	 * {@link Literal}.
	 **/
	Exp getFormatStringExp();

	/**
	 * Returns the value of the property named <code>propertyName</code>.
	 */
	Object getPropertyValue(String propertyName);

	/**
	 * Returns the definitions of the properties this member may have.
	 */
	Property[] getProperties();
}

// End Member.java
