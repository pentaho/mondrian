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


package mondrian.rolap.agg;

import mondrian.rolap.CellKey;
import mondrian.rolap.SqlStatement;
import mondrian.spi.SegmentBody;
import mondrian.util.Pair;

import java.util.*;

/**
 * A <code>SegmentDataset</code> holds the values in a segment.
 *
 * @author jhyde
 * @since 21 March, 2002
 */
public interface SegmentDataset extends Iterable<Map.Entry<CellKey, Object>> {
    /**
     * Returns the value at a given coordinate, as an {@link Object}.
     *
     * @param pos Coordinate position
     * @return Value
     */
    Object getObject(CellKey pos);

    /**
     * Returns the value at a given coordinate, as an {@code int}.
     *
     * @param pos Coordinate position
     * @return Value
     */
    int getInt(CellKey pos);

    /**
     * Returns the value at a given coordinate, as a {@code double}.
     *
     * @param pos Coordinate position
     * @return Value
     */
    double getDouble(CellKey pos);

    /**
     * Returns whether the cell at a given coordinate is null.
     *
     * @param pos Coordinate position
     * @return Whether cell value is null
     */
    boolean isNull(CellKey pos);

    /**
     * Returns whether there is a value at a given coordinate.
     *
     * @param pos Coordinate position
     * @return Whether there is a value
     */
    boolean exists(CellKey pos);

    /**
     * Returns the number of bytes occupied by this dataset.
     *
     * @return number of bytes
     */
    double getBytes();

    void populateFrom(int[] pos, SegmentDataset data, CellKey key);

    /**
     * Sets the value a given ordinal.
     *
     * @param pos Ordinal
     * @param rowList Row list
     * @param column Column of row list
     */
    void populateFrom(
        int[] pos, SegmentLoader.RowList rowList, int column);

    /**
     * Returns the SQL type of the data contained in this dataset.
     * @return A value of SqlStatement.Type
     */
    SqlStatement.Type getType();

    /**
     * Return an immutable, final and serializable implementation
     * of a SegmentBody in order to cache this dataset.
     *
     * @param axes An array with, for each axis, the set of axis values, sorted
     *     in natural order, and a flag saying whether the null value is also
     *     present.
     *     This is supplied by the {@link SegmentLoader}.
     *
     * @return A {@link SegmentBody}.
     */
    SegmentBody createSegmentBody(
        List<Pair<SortedSet<Comparable>, Boolean>> axes);
}

// End SegmentDataset.java
