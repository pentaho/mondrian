/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1999-2005 Kana Software, Inc. and others.
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
     **/
    Dimension[] getDimensions();

    /**
     * Finds a hierarchy whose name (or unique name, if <code>unique</code> is
     * true) equals <code>s</code>.
     **/
    Hierarchy lookupHierarchy(String s, boolean unique);

    /**
     * Returns Member[]. It builds Member[] by analyzing cellset, which
     * gets created by running mdx sQuery.  <code>query</code> has to be in the
     * format of something like "[with calculated members] select *members* on
     * columns from <code>this</code>".
     **/
    Member[] getMembersForQuery(String query, List calcMembers);

    /**
     * Returns the time dimension for this cube, or <code>null</code>
     * if there is no time dimension.
     */
    Dimension getTimeDimension();

    Level getYearLevel();
    Level getQuarterLevel();
    Level getMonthLevel();
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
