/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
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
    private final List valueList = new ArrayList();
    
    /** 
     * After all of the columns are loaded into the constrainedColumnList instance
     * variable, this columnsCache is created the first time the getColumns
     * method is called. 
     * <p>
     * It is assumed that the call to all additional columns, addConstrainedColumn,
     * will not be called after the first call to getColumns method.
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
     * @param constraint Constraint to apply, or null to add column to the
     *   output without applying constraint
     */
    public void addConstrainedColumn(
            RolapStar.Column column,
            ColumnConstraint constraint) {

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
            final ColumnConstraint prevValue =
                    (ColumnConstraint) valueList.get(index);
            if (prevValue == null) {
                // Previous column was unconstrained. Constrain on new value.
            } else if (constraint == null) {
                // Previous column was constrained. Nothing to do.
                return;
            } else if (constraint.equalConstraint(prevValue)) {
                // Same constraint again. Nothing to do.
                return;
            } else {
                // Different constraint. Request is impossible to satisfy.
                constraint = null;
                unsatisfiable = true;
            }
            valueList.set(index, constraint);

        } else {
            this.constrainedColumnList.add(column);
            this.constrainedColumnsBitKey.set(bitPosition);
            this.valueList.add(constraint);
        }
    }

    public RolapStar.Measure getMeasure() {
        return measure;
    }

    public RolapStar.Column[] getConstrainedColumns() {
        if (this.columnsCache == null) {
            // This is called more than once so caching the value makes
            // sense.
            makeColumnsCache();
        }
        return this.columnsCache;
    }

    private void makeColumnsCache() {
        // ignore the star, the 0th element of constrainedColumnList
        this.columnsCache = new RolapStar.Column[constrainedColumnList.size() - 1];
        for (int i = 0; i < this.columnsCache.length; i++) {
            columnsCache[i] = (RolapStar.Column) constrainedColumnList.get(i + 1);
        }
    }

    /**
     * Returns the BitKey for the list of columns.
     */
    public BitKey getConstrainedColumnsBitKey() {
        return constrainedColumnsBitKey;
    }

    public List getValueList() {
        return valueList;
    }

    public Object[] getSingleValues() {
        // Currently, this is called only once per CellRequest instance
        // so there is no need to cache the value.
        Object[] a = new Object[valueList.size()];
        for (int i = 0, n = valueList.size(); i < n; i++) {
            ColumnConstraint constr = (ColumnConstraint) valueList.get(i);
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
