/*
// $Id$
//
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
//
// jhyde, 1 March, 1999
*/

package mondrian.olap;

/**
 * Wrapper around an ADO MD level.
 **/
public interface Level extends OlapElement {

	int getDepth();
	Hierarchy getHierarchy();
	Member[] getMembers();
	Level getChildLevel();
	Level getParentLevel();
	boolean isAll();
	boolean areMembersUnique();
	Member[] getPeriodsToDate(Member member);
	int getLevelType();
	static final int STANDARD = 0;
	static final int YEARS = 1;
	static final int QUARTERS = 2;
	static final int MONTHS = 3;
	static final int WEEKS = 4;
	static final int DAYS = 5;
	/** Returns properties defined against this level. **/
	Property[] getProperties();
	/** Returns properties defined against this level and parent levels. **/
	Property[] getInheritedProperties();
}

// End Level.java
