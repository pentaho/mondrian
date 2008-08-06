/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2008 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
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
 * @version $Id$
 */
public class CellRequest {
    private final RolapStar.Measure measure;
    public final boolean extendedContext;
    public final boolean drillThrough;
    /**
     * List of {@link mondrian.rolap.RolapStar.Column}s being which have values
     * in this request.
     */
    private final List<RolapStar.Column> constrainedColumnList =
        new ArrayList<RolapStar.Column>();

    private final List<StarColumnPredicate> columnPredicateList =
        new ArrayList<StarColumnPredicate>();

    /*
     * Array of column values;
     * Not used to represent the compound members along one or more dimensions.
     */
    private Object[] singleValues;

    /**
     * After all of the columns are loaded into the constrainedColumnList instance
     * variable, this columnsCache is created the first time the getColumns
     * method is called.
     * <p>
     * It is assumed that the call to all additional columns,
     * {@link #addConstrainedColumn}, will not be called after the first call to
     * the {@link #getConstrainedColumns()} method.
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
     */
    private final Map<BitKey, StarPredicate> compoundPredicateMap =
        new LinkedHashMap<BitKey, StarPredicate>();

    /**
     * Whether the request is impossible to satisfy. This is set to 'true' if
     * contradictory constraints are applied to the same column. For example,
     * the levels [Customer].[City] and [Cities].[City] map to the same column
     * via the same join-path, and one constraint sets city = 'Burbank' and
     * another sets city = 'Los Angeles'.
     */
    private boolean unsatisfiable;

    /**
     * The columnPredicateList and columnsCache must be set after all constraints
     * have been added. This is used by access methods to determine if
     * both columnPredicateList and columnsCache need to be generated.
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

        final int bitPosition = column.getBitPosition();
        if (this.constrainedColumnsBitKey.get(bitPosition)) {
            // This column is already constrained. Unless the value is the same,
            // or this value or the previous value is null (meaning
            // unconstrained) the request will never return any results.
            int index = constrainedColumnList.indexOf(column);
            assert index >= 0;
            final StarColumnPredicate prevValue = columnPredicateList.get(index);
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
            columnPredicateList.set(index, predicate);

        } else {
            this.constrainedColumnList.add(column);
            this.constrainedColumnsBitKey.set(bitPosition);
            this.columnPredicateList.add(predicate);
        }
    }

    /**
     * Add compound member (formed via aggregate function) constraint to the
     * Cell.
     *
     * @param compoundBitKey
     * @param compoundPredicate
     */
    public void addAggregateList(
        BitKey compoundBitKey,
        StarPredicate compoundPredicate)
    {
        compoundPredicateMap.put(compoundBitKey, compoundPredicate);
    }

    public RolapStar.Measure getMeasure() {
        return measure;
    }

    public RolapStar.Column[] getConstrainedColumns() {
        if (this.columnsCache == null) {
            // This is called more than once so caching the value makes
            // sense.
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
     * Get the map of compound predicates
     * @return predicate map
     */
    public Map<BitKey, StarPredicate> getCompoundPredicateMap() {
        return compoundPredicateMap;
    }

    /**
     * Builds the {@link #columnsCache} and reorders the
     * {@link #columnPredicateList}
     * based upon bit key position of the columns.
     */
    private void check() {
        if (isDirty) {
            final int size = constrainedColumnList.size();
            this.columnsCache = new RolapStar.Column[size];
            final StarColumnPredicate[] oldColumnPredicates =
                columnPredicateList.toArray(
                    new StarColumnPredicate[columnPredicateList.size()]);
            columnPredicateList.clear();
            int i = 0;
            for (int bitPos : constrainedColumnsBitKey) {
                // NOTE: If the RolapStar.Column were stored in maybe a Map
                // rather than the constrainedColumnList List, we would
                // not have to for-loop over the list for each bit position.
                for (int j = 0; j < size; j++) {
                    RolapStar.Column rc = constrainedColumnList.get(j);
                    if (rc.getBitPosition() == bitPos) {
                        int index = constrainedColumnList.indexOf(rc);
                        final StarColumnPredicate value =
                            oldColumnPredicates[index];
                        columnPredicateList.add(value);
                        columnsCache[i++] = rc;
                        break;
                    }
                }
            }
            isDirty = false;
        }
    }

    public List<StarColumnPredicate> getValueList() {
        check();
        return columnPredicateList;
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
            singleValues = new Object[columnPredicateList.size()];
            final int size = columnPredicateList.size();
            for (int i = 0; i < size; i++) {
                ValueColumnPredicate predicate =
                    (ValueColumnPredicate) columnPredicateList.get(i);
                singleValues[i] = predicate.getValue();
            }
        }
        return singleValues;
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
