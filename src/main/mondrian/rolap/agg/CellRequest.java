/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/

package mondrian.rolap.agg;

import mondrian.rolap.RolapStar;
import mondrian.rolap.BitKey;

import java.util.ArrayList;
import java.util.List;


/**
 * A <code>CellRequest</code> contains the context necessary to get a cell
 * value from a star.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
public class CellRequest {
    private final RolapStar.Measure measure;
    /**
     * List of columns being which have values in this request. The 0th
     * entry is a dummy entry: the star, which ensures that 2 requests for the
     * same columns on measures in the same star get put into the same batch.
     *
     * We need to efficiently compare pairs of column lists (to figure out
     * whether two CellRequest objects belong to the same
     * FastBatchingCellReader.Batch), so we use an implementation of ArrayList
     * which computes hashCode and equals more efficiently.
     */
    //private final List columnList = new FastHashingArrayList();
    private final List columnList = new ArrayList();
    private final List valueList = new ArrayList();
    private RolapStar.Column[] columns = null;
    private BitKey bitKey;

    /** Creates a {@link CellRequest}. **/
    public CellRequest(RolapStar.Measure measure) {
        this.measure = measure;
        this.columnList.add(measure.getStar());
        this.bitKey =
            BitKey.Factory.makeBitKey(measure.getStar().getColumnCount());
    }

    public void addConstrainedColumn(RolapStar.Column column, Object value) {
        // for every column there MUST be a value (even if it is null)
        columnList.add(column);
        this.bitKey.setByPos(column.getBitPosition());
        if (value != null) {
            value = new ColumnConstraint(value);
        }
        valueList.add(value);
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
     **/
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
     * Extension to {@link ArrayList} with fast {@link #equals(Object)} and
     * {@link #hashCode()} methods.
     */
/*
    private static class FastHashingArrayList extends ArrayList {
        public boolean equals(Object o) {
            if (!(o instanceof FastHashingArrayList)) {
                return false;
            }
            FastHashingArrayList that = (FastHashingArrayList) o;
            final int size = this.size();
            if (size != that.size()) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                Object o1 = (Object) this.get(i);
                Object o2 = (Object) that.get(i);
                if (!o1.equals(o2)) {
                    return false;
                }
            }
            return true;
        }

        public int hashCode() {
            int hashCode = 1;
            for (int i = 0; i < size(); i++) {
                Object obj = (Object) get(i);
                hashCode = 31 * hashCode + (obj == null ? 0 : obj.hashCode());
            }
            return hashCode;
        }
    }
*/
}

// End CellRequest.java
