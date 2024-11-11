/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.olap;

import java.util.List;
import java.util.Set;

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
     */
    Dimension[] getDimensions();

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
     * element, as defined in <code>Mondrian.xml</code>.
     *
     * @param xml XML string
     */
    Member createCalculatedMember(String xml);

    /**
     * Finds out non joining dimensions for this cube.
     *
     * @param tuple array of members
     * @return Set of dimensions that do not exist (non joining) in this cube
     */
    Set<Dimension> nonJoiningDimensions(Member[] tuple);

    /**
     * Finds out non joining dimensions for this cube.
     *
     * @param otherDims Set of dimensions to be tested for existence
     *     in this cube
     * @return Set of dimensions that do not exist (non joining) in this cube
     */
    Set<Dimension> nonJoiningDimensions(Set<Dimension> otherDims);
}

// End Cube.java
