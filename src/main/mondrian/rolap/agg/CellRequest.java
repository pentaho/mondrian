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
    private final List columnList = new FastHashingArrayList();
    private final List valueList = new ArrayList();

    /** Creates a {@link CellRequest}. **/
    public CellRequest(RolapStar.Measure measure) {
        this.measure = measure;
        this.columnList.add(measure.getStar());
    }

    public void addConstrainedColumn(RolapStar.Column column, Object value) {
        columnList.add(column);
        if (value != null) {
            value = new ColumnConstraint(value);
        }
        valueList.add(value);
    }

    void addColumn(RolapStar.Column column) {
        addConstrainedColumn(column, null);
    }

    public RolapStar.Measure getMeasure() {
        return measure;
    }

    public RolapStar.Column[] getColumns() {
        // ignore the star, the 0th element of columnList
        RolapStar.Column[] a = new RolapStar.Column[columnList.size() - 1];
        for (int i = 0; i < a.length; i++) {
            a[i] = (RolapStar.Column) columnList.get(i + 1);
        }
        return a;
    }

    /** Returns a list which identifies which batch this request will
     * belong to. The list contains the star as well as the
     * columns. **/
    public List getBatchKey() {
        return columnList;
    }

    public List getValueList() {
        return valueList;
    }

    public Object[] getSingleValues() {
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
}

// End CellRequest.java
