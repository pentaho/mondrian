/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1999-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 2 March, 1999
*/

package mondrian.olap;

import java.util.List;

public interface Cube extends OlapElement {

    String getName();

    Schema getSchema();

    /**
     * Returns the dimensions of this cube.
     */
    Dimension[] getDimensions();

    /**
     * Finds a hierarchy whose name (or unique name, if <code>unique</code> is
     * true) equals <code>s</code>.
     */
    Hierarchy lookupHierarchy(String s, boolean unique);

    /**
     * Returns Member[]. It builds Member[] by analyzing cellset, which
     * gets created by running mdx sQuery.  <code>query</code> has to be in the
     * format of something like "[with calculated members] select *members* on
     * columns from <code>this</code>".
     */
    Member[] getMembersForQuery(String query, List<Member> calcMembers);

    /**
     * Returns the time dimension for this cube, or <code>null</code>
     * if there is no time dimension.
     */
    Dimension getTimeDimension();

    /** 
     * Helper method that returns the Year Level or returns null if the Time
     * Dimension does not exist or if Year is not defined in the Time Dimension.
     * 
     * @return Level or null.
     */
    Level getYearLevel();

    /** 
     * Return Quarter Level or null. 
     * 
     * @return Quarter Level or null. 
     */
    Level getQuarterLevel();

    /** 
     * Return Month Level or null. 
     * 
     * @return Month Level or null. 
     */
    Level getMonthLevel();

    /** 
     * Return Week Level or null. 
     * 
     * @return Week Level or null. 
     */
    Level getWeekLevel();

    /**
     * Returns a {@link SchemaReader} for which this cube is the context for
     * lookup up members.
     * If <code>role</code> is null, the returned schema reader also obeys the
     * access-control profile of role.
     */
    SchemaReader getSchemaReader(Role role);

    /**
     * Creates a calculated member in this cube.
     *
     * <p>The XML string must be a <code>&lt;CalculatedMember/&gt;</code>
     * element, as defined in <code>Mondrian.xml</code>.
     *
     * @param xml XML string
     */
    Member createCalculatedMember(String xml);
}

// End Cube.java
