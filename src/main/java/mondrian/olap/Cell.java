/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import org.olap4j.AllocationPolicy;
import org.olap4j.Scenario;

import java.util.List;

/**
 * A <code>Cell</code> is an item in the grid of a {@link Result}.  It is
 * returned by {@link Result#getCell}.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public interface Cell {
    /**
     * Returns the coordinates of this Cell in its {@link Result}.
     *
     * @return Coordinates of this Cell
     */
    List<Integer> getCoordinateList();

    /**
     * Returns the cell's raw value. This is useful for sending to further data
     * processing, such as plotting a chart.
     *
     * <p> The value is never null. It may have various types:<ul>
     *   <li>if the cell is null, the value is  {@link Util#nullValue};</li>
     *   <li>if the cell contains an error, the value is an instance of
     *       {@link Throwable};</li>
     *   <li>otherwise, the type of this value depends upon the type of
     *       measure: possible types include {@link java.math.BigDecimal},
     *       {@link Double}, {@link Integer} and {@link String}.</li>
     * </ul>
     *
     * @post return != null
     * @post (return instanceof Throwable) == isError()
     * @post (return instanceof Util.NullCellValue) == isNull()
     */
    Object getValue();

    /**
     * Return the cached formatted string, that survives an aggregate cache
     * clear.
     */
    String getCachedFormatString();

    /**
     * Returns the cell's value formatted according to the current format
     * string, and locale-specific settings such as currency symbol. The
     * current format string may itself be derived via an expression. For more
     * information about format strings, see {@link mondrian.util.Format}.
     */
    String getFormattedValue();

    /**
     * Returns whether the cell's value is null.
     */
    boolean isNull();

    /**
     * Returns whether the cell's calculation returned an error.
     */
    boolean isError();

    /**
     * Returns a SQL query that, when executed, returns drill through data
     * for this Cell.
     *
     * <p>If the parameter {@code extendedContext} is true, then the query will
     * include all the levels (i.e. columns) of non-constraining members
     * (i.e. members which are at the "All" level).
     *
     * <p>If the parameter {@code extendedContext} is false, the query will
     * exclude the levels (coulmns) of non-constraining members.
     *
     * <p>The result is null if the cell is based upon a calculated member.
     */
    String getDrillThroughSQL(boolean extendedContext);

    /**
     * Returns true if drill through is possible for this Cell.
     * Returns false if the Cell is based on a calculated measure.
     *
     * @return Whether can drill through on this cell
     */
    boolean canDrillThrough();

    /**
     * Returns the number of fact table rows which contributed to this Cell.
     */
    int getDrillThroughCount();

    /**
     * Returns the value of a property.
     *
     * @param propertyName Case-sensitive property name
     * @return Value of property
     */
    Object getPropertyValue(String propertyName);

    /**
     * Returns the context member for a particular dimension.
     *
     * The member is defined as follows (note that there is always a
     * member):<ul>
     *
     * <li>If the dimension appears on one of the visible axes, the context
     * member is simply the member on the current row or column.
     *
     * <li>If the dimension appears in the slicer, the context member is the
     * member of that dimension in the slier.
     *
     * <li>Otherwise, the context member is the default member of that
     * dimension (usually the 'all' member).</ul>
     *
     * @param hierarchy Hierarchy
     * @return current member of given hierarchy
     */
    Member getContextMember(Hierarchy hierarchy);

    /**
     * Helper method to implement {@link org.olap4j.Cell#setValue}.
     *
     * @param scenario Scenario
     * @param newValue New value
     * @param allocationPolicy Allocation policy
     * @param allocationArgs Arguments for allocation policy
     */
    void setValue(
        Scenario scenario,
        Object newValue,
        AllocationPolicy allocationPolicy,
        Object... allocationArgs);
}

// End Cell.java
