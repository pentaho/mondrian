/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho and others
// All Rights Reserved.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

import mondrian.rolap.*;

import java.util.*;

/**
 * A <code>CellRequest</code> contains the context necessary to get a cell
 * value from a star.
 *
 * @author jhyde
 * @since 21 March, 2002
 */
public class CellRequest {
    private final RolapStar.Measure measure;
    public final boolean extendedContext;
    public final boolean drillThrough;

    /*
     * Sparsely populated array of column predicates.  Each predicate will
     * be located according to the bitPosition of the column to which it
     * corresponds.  This costs a little memory in terms of unused array
     * slots, but avoids the need to explicitly sort the column predicates
     * into a canonical order. There aren't usually a lot of predicates to
     * sort, but that time adds up quickly.
     */
    private StarColumnPredicate[] sparseColumnPredicateList;

    /**
     * An array that contains the bit positions of each constrained
     * column.
     *
     * <p>Used to allow us to convert from a numeric index (i.e., give me the
     * third constrained column) to the actual column as referenced
     * by bit position.
     *
     * For example, if the three constrained columns have bitPositions 3, 16,
     * and 21, the contents of this array should be [3,16,21].
     */
    private int[] columnBitPositions;

    /**
     * Tracks the number of column constraints actually associated with this
     * CellRequest. We could figure this out by iterating over
     * sparseColumnPredicateList, but it's quicker to just track them
     * as they are added.
     */
    private int numColumns;

    /**
     * Reference back to the CellRequest's star.  All CellRequests in a
     * given query are associated with a single star. Keeping this
     * reference allows us to maintain a list of columns by bit position
     * in just one place (the star) rather than duplicate that
     * information in each CellRequest.
     */
    private RolapStar star = null;

    /*
     * Array of column values;
     * Not used to represent the compound members along one or more dimensions.
     */
    private Object[] singleValues;

    /**
     * After all of the columns are loaded, the columnsCache is created
     * the first time the getColumns method (or any method that itself
     * calls the check method) is called.
     *
     * <p>It is assumed that the call to all additional columns,
     * {@link #addConstrainedColumn}, will not be called after the first call
     * to the {@link #getConstrainedColumns()} method.
     *
     * <p>TODO: Since the expectation is that the columns do not change once
     * set, it may be worth either caching these structures so that only one
     * exists for every unique combination, or eliminating the cache and
     * creating a wrapper that makes the bit key + star's column list look
     * like a column array.
     */
    private RolapStar.Column[] columnsCache = null;

    /**
     * A bit is set for each column in the column list. Allows us to rapidly
     * figure out whether two requests are for the same column set.
     * These are all of the columns that are involved with a query, that is, all
     * required to be present in an aggregate table for the table be used to
     * fulfill the query.
     */
    private final BitKey constrainedColumnsBitKey;

    /**
     * Map from BitKey (representing a group of columns that forms a
     * compound key) to StarPredicate (representing the predicate
     * defining the compound member).
     *
     * <p>We use LinkedHashMap so that the entries occur in deterministic
     * order; otherwise, successive runs generate different SQL queries.
     * Another solution worth considering would be to use the inherent ordering
     * of BitKeys and create a sorted map.
     *
     * <p>Creating CellRequests is one of the top hotspots in Mondrian.
     * Therefore we initialize the map to null, and don't create a map until
     * we add the first entry.
     *
     * <p>The map (when not null) is sorted by key, to allow more rapid
     * comparison with maps of other requests and with existing segments.</p>
     */
    private SortedMap<BitKey, StarPredicate> compoundPredicateMap = null;

    /**
     * Whether the request is impossible to satisfy. This is set to 'true' if
     * contradictory constraints are applied to the same column. For example,
     * the levels [Customer].[City] and [Cities].[City] map to the same column
     * via the same join-path, and one constraint sets city = 'Burbank' and
     * another sets city = 'Los Angeles'.
     */
    private boolean unsatisfiable;

    /**
     * The columnPredicateList and columnsCache must be set after all
     * constraints have been added. This is used by access methods to determine
     * if both columnPredicateList and columnsCache need to be generated.
     */
    private boolean isDirty = true;

    /**
     * Creates a {@link CellRequest}.
     *
     * @param measure Measure the request is for
     * @param extendedContext If a drill-through request, whether to join in
     *   unconstrained levels so as to display extra columns
     * @param drillThrough Whether this is a request for a drill-through set
     */
    public CellRequest(
        RolapStar.Measure measure,
        boolean extendedContext,
        boolean drillThrough)
    {
        this.measure = measure;
        this.extendedContext = extendedContext;
        this.drillThrough = drillThrough;
        this.constrainedColumnsBitKey =
            BitKey.Factory.makeBitKey(measure.getStar().getColumnCount());
        this.sparseColumnPredicateList =
            new StarColumnPredicate[measure.getStar().getColumnCount()];
    }

    /**
     * Adds a constraint to this request.
     *
     * @param column Column to constraint
     * @param predicate Constraint to apply, or null to add column to the
     *   output without applying constraint
     */
    public final void addConstrainedColumn(
        RolapStar.Column column,
        StarColumnPredicate predicate)
    {
        assert columnsCache == null;

        // Sanity check; we should never be adding column constraints
        // from more than one star
        if (star == null) {
            star = column.getStar();
        } else {
            assert (star == column.getStar());
        }

        final int bitPosition = column.getBitPosition();
        if (this.constrainedColumnsBitKey.get(bitPosition)) {
            // This column is already constrained. Unless the value is the
            // same, or this value or the previous value is null (meaning
            // unconstrained) the request will never return any results.
            final StarColumnPredicate prevValue =
                sparseColumnPredicateList[bitPosition];
            if (prevValue == null) {
                // Previous column was unconstrained. Constrain on new
                // value.
            } else if (predicate == null) {
                // Previous column was constrained. Nothing to do.
                return;
            } else if (predicate.equalConstraint(prevValue)) {
                // Same constraint again. Nothing to do.
                return;
            } else {
                // Different constraint. Request is impossible to satisfy.
                predicate = null;
                unsatisfiable = true;
            }
        } else {
            this.constrainedColumnsBitKey.set(bitPosition);
            numColumns++;
        }

        // Note: it is possible and valid for predicate to be null here
        this.sparseColumnPredicateList[bitPosition] = predicate;
    }

    /**
     * Add compound member (formed via aggregate function) constraint to the
     * Cell.
     *
     * @param compoundBitKey Compound bit key
     * @param compoundPredicate Compound predicate
     */
    public void addAggregateList(
        BitKey compoundBitKey,
        StarPredicate compoundPredicate)
    {
        if (compoundPredicateMap == null) {
            compoundPredicateMap = new TreeMap<BitKey, StarPredicate>();
        }
        compoundPredicateMap.put(compoundBitKey, compoundPredicate);
    }

    /**
     * Returns the measure of this cell request.
     *
     * @return Measure
     */
    public RolapStar.Measure getMeasure() {
        return measure;
    }

    public RolapStar.Column[] getConstrainedColumns() {
        if (this.columnsCache == null) {
            // This is called more than once so caching the value makes sense.
            check();
        }
        return this.columnsCache;
    }

    /**
     * Returns the BitKey for the list of columns.
     *
     * @return BitKey for the list of columns
     */
    public BitKey getConstrainedColumnsBitKey() {
        return constrainedColumnsBitKey;
    }

    /**
     * Returns the map of compound predicates, or null if empty.
     *
     * <p>NOTE: It is not generally considered good API design to return null
     * to represent empty collections, but this collection is very often empty
     * and the the implementation of Collections.emptyMap().keySet().iterator()
     * is slow, so we optimize for the common case.
     *
     * @return predicate map, or null if empty
     */
    SortedMap<BitKey, StarPredicate> getCompoundPredicateMap() {
        return compoundPredicateMap;
    }

    /**
     * Builds the {@link #columnsCache} and {@link #columnBitPositions}
     * based upon bit key position of the columns.
     */
    private void check() {
        if (isDirty) {
            columnsCache = new RolapStar.Column[numColumns];
            columnBitPositions = new int[numColumns];
            int i = 0;
            for (int bitPos = constrainedColumnsBitKey.nextSetBit(0);
                bitPos >= 0;
                bitPos = constrainedColumnsBitKey.nextSetBit(bitPos + 1))
            {
                columnBitPositions[i] = bitPos;
                columnsCache[i] = this.star.getColumn(bitPos);
                i++;
            }
            isDirty = false;
        }
    }

    /**
     * Return the predicate value associated with the given index.  Note that
     * index is different than bit position; if there are three constraints then
     * the indices are 0, 1, and 2, while the bitPositions could span a larger
     * range.
     *
     * <p> It is valid for the predicate at a given index to be null (there
     * should always be a column at that index, but it may not have an
     * associated predicate).
     *
     * @param index Index of the constraint we're looking up
     * @return predicate value associated with the given index
     */
    public StarColumnPredicate getValueAt(int index) {
        check();
        return sparseColumnPredicateList[columnBitPositions[index]];
    }

    /**
     * Return the number of column constraints associated with this CellRequest.
     *
     * @return number of columns in the CellRequest
     */
    public int getNumValues() {
        check();
        return numColumns;
    }

    /**
     * Returns an array of the values for each column.
     *
     * <p>The caller must check whether this request is satisfiable before
     * calling this method. May throw {@link NullPointerException} if request
     * is not satisfiable.
     *
     * @pre !isUnsatisfiable()
     * @return Array of values for each column
     */
    public Object[] getSingleValues() {
        assert !unsatisfiable;
        if (singleValues == null) {
            check();
            singleValues = new Object[numColumns];
            int i = 0;
            for (int bitPos : columnBitPositions) {
                ValueColumnPredicate predicate =
                    (ValueColumnPredicate) sparseColumnPredicateList[bitPos];
                singleValues[i++] = predicate.getValue();
            }
        }
        return singleValues;
    }

    /**
     * Builds a map of column names to values, as specified
     * by this cell request object.
     */
    public Map<String, Comparable> getMappedCellValues() {
        final Map<String, Comparable> map =
            new HashMap<String, Comparable>();
        final RolapStar.Column[] columns =
            this.getConstrainedColumns();
        final Object[] values = this.getSingleValues();
        for (int i = 0; i < columns.length; i++) {
            RolapStar.Column column = columns[i];
            final Object o = values[i];
            map.put(
                column.getExpression().toSql(),
                o == RolapUtil.sqlNullValue
                    ? null
                    : (Comparable<?>) o);
        }
        return map;
    }

    /**
     * Returns whether this cell request is impossible to satisfy.
     * This occurs when the same column has two or more inconsistent
     * constraints.
     *
     * @return whether this cell request is impossible to satisfy
     */
    public boolean isUnsatisfiable() {
        return unsatisfiable;
    }
}

// End CellRequest.java
