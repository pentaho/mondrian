/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import javax.sql.DataSource;
import java.util.List;
import java.io.PrintWriter;

/**
 * API for controlling the contents of the cache.
 *
 * @see mondrian.olap.Connection#getCacheControl(java.io.PrintWriter)
 *
 * @author jhyde
 * @version $Id$
 * @since Sep 27, 2006
 */
public interface CacheControl {

    /**
     * Creates a region consisting of a single member.
     *
     * @param member Member
     * @param descendants If true, include descendants of the member in the
     *   region
     */
    CellRegion createMemberRegion(Member member, boolean descendants);

    /**
     * Creates a region consisting of a range between two members.
     *
     * <p>The members must belong to the same level of the same hierarchy.
     * One of the bounds may be null.
     *
     * <p>For example, given
     *
     * <code><pre>Member member97Q3; // [Time].[1997].[Q3]
     * Member member98Q2; // [Time].[1998].[Q2]
     * </pre></code>
     *
     * then
     *
     * <table border="1">
     * <tr>
     * <th>Expression</th>
     * <th>Meaning</th>
     * </tr>
     *
     * <tr>
     * <td>
     * <code>createMemberRegion(true, member97Q3, true, member98Q2, false)</code>
     * </td>
     * <td>The members between 97Q3 and 98Q2, inclusive:<br/>
     * [Time].[1997].[Q3],<br/>
     * [Time].[1997].[Q4],<br/>
     * [Time].[1998].[Q1],<br/>
     * [Time].[1998].[Q2]</td>
     * </tr>
     *
     * <tr>
     * <td>
     * <code>createMemberRegion(true, member97Q3, false, member98Q2, false)</code>
     * </td>
     * <td>The members between 97Q3 and 98Q2, exclusive:<br/>
     * [Time].[1997].[Q4],<br/>
     * [Time].[1998].[Q1]</td>
     * </tr>
     *
     * <tr>
     * <td>
     * <code>createMemberRegion(true, member97Q3, false, member98Q2, false)</code>
     * </td>
     * <td>The members between 97Q3 and 98Q2, including their descendants, and
     * including the lower bound but not the upper bound:<br/>
     * [Time].[1997].[Q3],<br/>
     * [Time].[1997].[Q3].[7],<br/>
     * [Time].[1997].[Q3].[8],<br/>
     * [Time].[1997].[Q3].[9],<br/>
     * [Time].[1997].[Q4],<br/>
     * [Time].[1997].[Q4].[10],<br/>
     * [Time].[1997].[Q4].[11],<br/>
     * [Time].[1997].[Q4].[12],<br/>
     * [Time].[1998].[Q1],<br/>
     * [Time].[1998].[Q1].[1],<br/>
     * [Time].[1998].[Q1].[2],<br/>
     * [Time].[1998].[Q1].[3]</td>
     * </tr>
     * </table>
     *
     * @param lowerInclusive Whether the the range includes the lower bound;
     *   ignored if the lower bound is not specified
     * @param lowerMember Lower bound member.
     *   If null, takes all preceding members
     * @param upperInclusive Whether the the range includes the upper bound;
     *   ignored if the upper bound is not specified
     * @param upperMember upper bound member.
     *   If null, takes all preceding members
     * @param descendants If true, include descendants of the member in the
     *   region
     */
    CellRegion createMemberRegion(
        boolean lowerInclusive,
        Member lowerMember,
        boolean upperInclusive,
        Member upperMember,
        boolean descendants);

    /**
     * Creates a region consisting of the cartesian product of two or more
     * regions.
     */
    CellRegion createCrossjoinRegion(CellRegion... regions);

    /**
     * Creates a region consisting of the union of two or more regions.
     *
     * <p>The regions must have the same dimensionality.
     *
     * @param regions Cell regions
     */
    CellRegion createUnionRegion(CellRegion... regions);

    /**
     * Creates a region consisting of all measures in a given cube.
     */
    CellRegion createMeasuresRegion(Cube cube);

    /**
     * Atomically all cells in the cache which correspond to measures in
     * a cube and in a given region.
     *
     * @param region Region
     */
    void flush(CellRegion region);

    /**
     * Prints the state of the cache as it pertains to a given region.
     */
    void printCacheState(PrintWriter pw, CellRegion region);

    /**
     * Prints a debug message.
     */
    void trace(String message);

    /**
     * Flushes the cache which maps schema URLs to metadata.
     *
     * <p>This cache is referenced only when creating a new connection, so
     * existing connections will continue to use the same schema definition.
     */
    void flushSchemaCache();

    // todo: document
    void flushSchema(
        final String catalogUrl,
        final String connectionKey,
        final String jdbcUser,
        String dataSourceStr);

    // todo: document
    void flushSchema(
        String catalogUrl,
        DataSource dataSource);

    /**
     * Region of cells.
     */
    public interface CellRegion {
        /**
         * Returns the dimensionality of a region.
         * @return A list of {@link mondrian.olap.Dimension} objects.
         */
        List<Dimension> getDimensionality();
    }
}

// End CacheControl.java
