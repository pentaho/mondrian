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
import java.util.Hashtable;
import java.util.Vector;

public interface Cube extends OlapElement, NameResolver {

	String getName();

	Connection getConnection();

	/**
	 * Returns the dimensions of this cube.
	 **/
	Dimension[] getDimensions();

	/**
	 * Finds a hierarchy whose name (or unique name, if <code>unique</code> is
	 * true) equals <code>s</code>.
	 **/
	Hierarchy lookupHierarchy(String s, boolean unique);

	/**
	 * Finds a level whose unique name is <code>s</code>. For example,
	 * "[Customers].[State]", not "Customers.State".
	 **/
	Level lookupLevel(String s);

	/**
	 * Returns direct children for every member in array. This bulk method may
	 * be much more efficient than {@link #lookupChild}.
	 **/
	Member[] getMemberChildren(Member[] parentMembers);

	/**
	 * Returns Member[]. It builds Member[] by analyzing cellset, which
	 * gets created by running mdx sQuery.  <code>query</code> has to be in the
	 * format of something like "[with calculated members] select *members* on
	 * columns from <code>this</code>".
	 **/
	Member[] getMembersForQuery(String query, Vector calcMembers);

	/**
	 * Looks up a {@link Member} for each unique name in
	 * <code>memberNames</code>, and places them in a hash table.
	 **/
	void lookupMembers(Vector memberNames, Hashtable mapNameToMember);

	Level getYearLevel();
	Level getQuarterLevel();
	Level getMonthLevel();
	Level getWeekLevel();
	Member[] getMeasures();
}

// End Cube.java
