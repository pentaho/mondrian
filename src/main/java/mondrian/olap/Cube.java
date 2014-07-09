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
 * Cube.
 *
 * @author jhyde, 2 March, 1999
 */
public interface Cube extends OlapElement, Annotated {

    String getName();

    Schema getSchema();

    /**
     * Returns the dimensions of this cube.
     *
     * @deprecated Use {@link #getDimensionList()}; will be removed before 4.0.
     */
    Dimension[] getDimensions();

    /**
     * Returns the dimensions in this cube.
     *
     * @return List of dimensions
     */
    List<? extends Dimension> getDimensionList();

    /**
     * Returns the named sets of this cube.
     */
    NamedSet[] getNamedSets();

    /**
     * Finds a hierarchy whose name (or unique name, if <code>unique</code> is
     * true) equals <code>s</code>.
     */
    Hierarchy lookupHierarchy(Id.NameSegment s, boolean unique);

    /**
     * Returns Member[]. It builds Member[] by analyzing cellset, which
     * gets created by running mdx sQuery.  <code>query</code> has to be in the
     * format of something like "[with calculated members] select *members* on
     * columns from <code>this</code>".
     */
    Member[] getMembersForQuery(String query, List<Member> calcMembers);

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
     * element, as defined in <code>MondrianSchema.xml</code>.
     *
     * @param xml XML string
     */
    Member createCalculatedMember(String xml);

    /**
     * Returns the first level of a given type in this cube.
     *
     * @param levelType Level type
     * @return First level of given type, or null
     */
    Level getTimeLevel(org.olap4j.metadata.Level.Type levelType);
}

// End Cube.java
