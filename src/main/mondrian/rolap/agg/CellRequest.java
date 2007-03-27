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
     * in this request. The 0th entry is a dummy entry: the star, which ensures
     * that 2 requests for the same columns on measures in the same star get
     * put into the same batch.
     */
    private final List constrainedColumnList = new ArrayList();
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

    /**
     * Creates a {@link CellRequest}.
     */
    public CellRequest(
            RolapStar.Measure measure,
            boolean extendedContext,
            boolean drillThrough) {
        this.measure = measure;
        this.extendedContext = extendedContext;
        this.drillThrough = drillThrough;
        this.constrainedColumnList.add(measure.getStar());
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
            StarColumnPredicate predicate) {

        assert columnsCache == null;

        final int bitPosition = column.getBitPosition();
        if (this.constrainedColumnsBitKey.get(bitPosition)) {
            // This column is already constrained. Unless the value is the same,
            // or this value or the previous value is null (meaning
            // unconstrained) the request will never return any results.
            int index = constrainedColumnList.indexOf(column);
            Util.assertTrue(index >= 0);
            // column list has RolapStar as its first element
            --index;
            final StarColumnPredicate prevValue = valueList.get(index);
            if (prevValue == null) {
                // Previous column was unconstrained. Constrain on new value.
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
     */
    public BitKey getConstrainedColumnsBitKey() {
        return constrainedColumnsBitKey;
    }

    /** 
     * This method builds both the columnsCache and reorders the valueList
     * based upon bit key position of the columns.
     */
    private void check() {
        if (isDirty) {
            final int size = constrainedColumnList.size();
            this.columnsCache = new RolapStar.Column[size - 1];
            List<StarColumnPredicate> vl = new ArrayList<StarColumnPredicate>();
            int cnt = 0;
            for (int bitPos : constrainedColumnsBitKey) {
                // NOTE: If the RolapStar.Column were stored in maybe a Map
                // rather than the constrainedColumnList List, we would
                // not have to for-loop over the list for each bit position.
                for (int j = 1; j < size; j++) {
                    RolapStar.Column rc = 
                            (RolapStar.Column) constrainedColumnList.get(j);
                    if (rc.getBitPosition() == bitPos) {
                        int index = constrainedColumnList.indexOf(rc) - 1;
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

    public Object[] getSingleValues() {
        // if the cell request is unsatisfiable, null pointers
        // may happen in this code.  The caller should
        // check unsatisfiable first before calling.
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
     */
    public boolean isUnsatisfiable() {
        return unsatisfiable;
    }
}

// End CellRequest.java
