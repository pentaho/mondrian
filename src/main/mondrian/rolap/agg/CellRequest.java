/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/

package mondrian.rolap.agg;

import mondrian.rolap.*;
import mondrian.olap.Util;

import java.util.ArrayList;
import java.util.List;

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
    private List<StarColumnPredicate> valueList =
        new ArrayList<StarColumnPredicate>();

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
     * Whether the request is impossible to satisfy. This is set to 'true' if
     * contradictory constraints are applied to the same column. For example,
     * the levels [Customer].[City] and [Cities].[City] map to the same column
     * via the same join-path, and one constraint sets city = 'Burbank' and
     * another sets city = 'Los Angeles'.
     */
    private boolean unsatisfiable;

    /**
     * The valueList and columnsCache must be set after all constraints
     * have been added. This is used by access methods to determine if
     * both valueList and columnsCache need to be generated.
     */
    private boolean isDirty = true;

    private List<StarPredicate> predicateList = new ArrayList<StarPredicate>();

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
            boolean drillThrough) {
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
    public void addConstrainedColumn(
        RolapStar.Column column,
        StarColumnPredicate predicate)
    {
        addConstrainedColumn(column, predicate, Mode.AND);
    }

    /**
     * Adds a constraint to this request, applying a given operation if the
     * column is already constrained.
     *
     * @param column Column to constraint
     * @param predicate Constraint to apply, or null to add column to the
     *   output without applying constraint
     * @param mode Whether to combine with existing constraint using AND or OR
     */
    public void addConstrainedColumn(
        RolapStar.Column column,
        StarColumnPredicate predicate,
        Mode mode)
    {
        assert columnsCache == null;

        final int bitPosition = column.getBitPosition();
        if (this.constrainedColumnsBitKey.get(bitPosition)) {
            // This column is already constrained. Unless the value is the same,
            // or this value or the previous value is null (meaning
            // unconstrained) the request will never return any results.
            int index = constrainedColumnList.indexOf(column);
            assert index >= 0;
            final StarColumnPredicate prevValue = valueList.get(index);
            if (prevValue == null) {
                // Previous column was unconstrained. Constrain on new
                // value.
            } else if (predicate == null) {
                // Previous column was constrained. Nothing to do.
                assert mode == Mode.AND : "FIXME: OR case";
                return;
            } else if (predicate.equalConstraint(prevValue)) {
                        // Same constraint again. Nothing to do.
                        return;
            } else {
                switch (mode) {
                case AND:
                    // Different constraint. Request is impossible to satisfy.
                    predicate = null;
                    unsatisfiable = true;
                    break;

                case OR:
                    predicate = predicate.orColumn(prevValue);
                    break;

                default:
                    throw Util.unexpected(mode);
                }
            }
            valueList.set(index, predicate);

        } else {
            this.constrainedColumnList.add(column);
            this.constrainedColumnsBitKey.set(bitPosition);
            this.valueList.add(predicate);
        }
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
     * Builds the columnsCache and reorders the valueList
     * based upon bit key position of the columns.
     */
    private void check() {
        if (isDirty) {
            final int size = constrainedColumnList.size();
            this.columnsCache = new RolapStar.Column[size];
            List<StarColumnPredicate> vl = new ArrayList<StarColumnPredicate>();
            int cnt = 0;
            for (int bitPos : constrainedColumnsBitKey) {
                // NOTE: If the RolapStar.Column were stored in maybe a Map
                // rather than the constrainedColumnList List, we would
                // not have to for-loop over the list for each bit position.
                for (int j = 0; j < size; j++) {
                    RolapStar.Column rc = constrainedColumnList.get(j);
                    if (rc.getBitPosition() == bitPos) {
                        int index = constrainedColumnList.indexOf(rc);
                        final StarColumnPredicate value = valueList.get(index);
                        vl.add(value);
                        columnsCache[cnt++] = rc;
                        break;
                    }
                }
            }
            valueList = vl;

            isDirty = false;
        }
    }

    public List<StarColumnPredicate> getValueList() {
        check();
        return valueList;
    }

    /**
     * Returns a list of all predicates which are not associated with a column.
     *
     * @return list of predicates not associated with a column
     */
    public List<StarPredicate> getPredicateList() {
        return predicateList;
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
        check();
        // Currently, this is called only once per CellRequest instance
        // so there is no need to cache the value.
        Object[] a = new Object[valueList.size()];
        for (int i = 0, n = valueList.size(); i < n; i++) {
            ValueColumnPredicate constr =
                (ValueColumnPredicate) valueList.get(i);
            a[i] = constr.getValue();
        }
        return a;
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

    /**
     * Adds a predicate to the 'ad hoc' predicates which are not associated
     * with any column.
     *
     * <p>Caller must call this method with a non-descending sequence of values
     * for 'groupOrdinal'. In the following example, the caller uses the
     * sequence (0, 0, 1) to create two groups.
     *
     * <blockquote><pre>
     * addConstraint("year = 1997 and quarter = Q1", 0)
     * addConstraint("year = 1997 and quarter = Q3 and month = July", 0)
     * addConstraint("nation = USA", 1)
     * </pre></blockquote>
     *
     * would create the SQL constraint
     *
     * <blockquote><pre>
     *  ( ( year = 1997 and quarter = Q1 ) or
     *    ( year = 1997 and quarter = Q3 and month = July ) ) and
     *  ( nation = USA )
     * </pre></blockquote>
     *
     * <p>It is illegal for a sequence to have
     * a missing value (0, 2) or a negative value (-1, 0).
     *
     * @param predicate Predicate
     * @param groupOrdinals Location in tree where predicate is to be added
     */
    public void addConstraint(StarPredicate predicate, int[] groupOrdinals) {
        final int size0 = predicateList.size();
        if (groupOrdinals[0] == size0) {
            // new predicate
            predicateList.add(predicate);
        } else if (groupOrdinals[0] == size0 - 1) {
            // they want to OR the last predicate in the level#0 list with this
            // new predicate
            StarPredicate lastPredicate0 =
                predicateList.get(size0 - 1);
            int size1 = lastPredicate0 instanceof OrPredicate ?
                ((OrPredicate) lastPredicate0).getChildren().size() :
                1;
            if (groupOrdinals[1] == size1) {
                // new predicate
                StarPredicate newPredicate = lastPredicate0.or(predicate);
                predicateList.set(size0 - 1, newPredicate);
            } else if (groupOrdinals[1] == size1 - 1) {
                // they want to AND the last predicate in the level#1 list with
                // this new predicate
                StarPredicate lastPredicate1 =
                    lastPredicate0 instanceof OrPredicate ?
                        ((OrPredicate) lastPredicate0).getChildren().get(size1 - 1) :
                        lastPredicate0;
                int size2 = lastPredicate1 instanceof AndPredicate ?
                    ((AndPredicate) lastPredicate1).getChildren().size() :
                    1;
                if (groupOrdinals[2] == size2) {
                    // new predicate
                    StarPredicate newPredicate1 = lastPredicate1.and(predicate);
                    final List<StarPredicate> list =
                        new ArrayList<StarPredicate>();
                    if (lastPredicate0 instanceof OrPredicate) {
                        list.addAll(((OrPredicate) lastPredicate0).getChildren());
                    } else {
                        list.add(lastPredicate0);
                    }
                    list.set(size1 - 1, newPredicate1);
                    StarPredicate newPredicate0 = new OrPredicate(list);
                    predicateList.set(size0 - 1, newPredicate0);
                } else if (groupOrdinals[2] == size2 - 1) {
                    assert false;
                }
            } else {
                final StarPredicate newPredicate = lastPredicate0.and(predicate);
                predicateList.set(size0 - 1, newPredicate);
            }
        } else {
            throw Util.newInternal("invalid ordinal " + groupOrdinals +
                " when predicateList.size()=" + size0);
        }
    }

    public enum Mode { AND, OR }
}

// End CellRequest.java
