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
    private final List columnList = new ArrayList();
    private final List valueList = new ArrayList();
    private RolapStar.Column[] columns = null;
    /**
     * A bit is set for each column in the column list. Allows us to rapidly
     * figure out whether two requests are for the same column set.
     */
    private final BitKey bitKey;
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
        this.columnList.add(measure.getStar());
        this.bitKey =
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
        final int bitPosition = column.getBitPosition();
        if (this.bitKey.get(bitPosition)) {
            // This column is already constrained. Unless the value is the same,
            // or this value or the previous value is null (meaning
            // unconstrained) the request will never return any results.
            int index = columnList.indexOf(column);
            Util.assertTrue(index >= 0);
            --index; // column list has dummy first element
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
            return;
        }

        this.columnList.add(column);
        this.bitKey.set(bitPosition);
        this.valueList.add(constraint);
    }

    public RolapStar.Measure getMeasure() {
        return measure;
    }

    public RolapStar.Column[] getColumns() {
        if (this.columns == null) {
            // This is called more than once so caching the value makes
            // sense.
            makeColumns();
        }
        return this.columns;
    }

    private void makeColumns() {
        // ignore the star, the 0th element of columnList
        this.columns = new RolapStar.Column[columnList.size() - 1];
        for (int i = 0; i < this.columns.length; i++) {
            columns[i] = (RolapStar.Column) columnList.get(i + 1);
        }
    }

    /**
     * Returns the BitKey for the list of columns.
     */
    public BitKey getBatchKey() {
        return bitKey;
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
