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
    /** List of columns being which have values in this request. The 0th
     * entry is a dummy entry: the star, which ensures that 2 requests for the
     * same columns on measures in the same star get put into the same batch.
     */
    private final List columnList;
    private final List valueList;

    /** Creates a {@link CellRequest}. **/
    public CellRequest(RolapStar.Measure measure) {
        this.measure = measure;
        this.columnList = new ArrayList();
        this.valueList = new ArrayList();

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
}

// End CellRequest.java
