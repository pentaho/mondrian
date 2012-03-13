/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.spi;

import mondrian.olap.Util;
import mondrian.util.ArraySortedSet;

import java.io.Serializable;
import java.util.SortedSet;


/**
 * Constrained columns are part of the SegmentHeader and SegmentCache.
 * They uniquely identify a constrained column within a segment.
 * Each segment can have many constrained columns. Each column can
 * be constrained by multiple values at once (similar to a SQL in()
 * predicate).
 *
 * <p>They are immutable and serializable.
 */
public class SegmentColumn implements Serializable {
    private static final long serialVersionUID = -5227838916517784720L;
    public final String columnExpression;
    public final int valueCount;
    public final SortedSet<Comparable> values;
    private final int hashCode;

    /**
     * Creates a SegmentColumn.
     *
     * @param columnExpression SQL expression for the column. Unique within the
     *     star schema, including accesses to the same physical column via
     *     different join paths.
     *
     * @param valueCount Number of distinct values of this column in the
     *     database. For these purposes, null is counted as a value. If there
     *     are N distinct values of the column, and we have a collection of
     *     segments that cover N values, then Mondrian assumes that it is safe
     *     to roll up.
     *
     * @param valueList List of values to constrain the
     *     column to, or null if unconstrained. Values must be
     *     {@link Comparable} and immutable. For example, Integer, Boolean,
     *     String or Double.
     */
    public SegmentColumn(
        String columnExpression,
        int valueCount,
        SortedSet<Comparable> valueList)
    {
        this.columnExpression = columnExpression;
        this.valueCount = valueCount;
        this.values = valueList;
        this.hashCode = computeHashCode();
    }

    private int computeHashCode() {
        return Util.hash(
            this.columnExpression.hashCode(),
            this.values);
    }

    /**
     * Merges this column with another
     * resulting in another whose values are super set of both.
     */
    public SegmentColumn merge(SegmentColumn col) {
        assert col != null;
        assert col.columnExpression.equals(this.columnExpression);

        // If any values are wildcard, the merged result is a wildcard.
        if (this.values == null || col.values == null) {
            return new SegmentColumn(
                columnExpression,
                valueCount,
                null);
        }

        return new SegmentColumn(
            columnExpression,
            valueCount,
            ((ArraySortedSet) this.values).merge(
                (ArraySortedSet) col.values));
    }

    /**
     * Returns the column expression of this constrained column.
     * @return A column expression.
     */
    public String getColumnExpression() {
        return columnExpression;
    }

    /**
     * Returns an array of predicate values for this column.
     * @return An array of object values.
     */
    public SortedSet<Comparable> getValues() {
        return values;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SegmentColumn)) {
            return false;
        }
        SegmentColumn that = (SegmentColumn) obj;
        if (this.values == null && that.values == null) {
            return true;
        }
        return this.columnExpression.equals(that.columnExpression)
            && Util.equals(this.values, that.values);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    /**
     * Returns the number of distinct values that occur for this column in the
     * database.
     *
     * <p>Mondrian uses this to know that it can safely combine multiple
     * segments to roll up. For example, if for the "quarter" column, one
     * segment has values {"Q1", "Q2"} and another has values {"Q3", "Q4"},
     * and Mondrian knows that there are 4 values, then it can roll up.
     *
     * <p>If this method returns a value that is too low, Mondrian may generate
     * incorrect results. If you don't know the number of values, return -1.</p>
     *
     * @return Number of distinct values, including null, that occur for this
     * column
     */
    public int getValueCount() {
        return valueCount;
    }
}

// End SegmentColumn.java
