/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/

package mondrian.rolap.agg;

import mondrian.olap.Util;
import mondrian.rolap.RolapStar;

import java.util.ArrayList;

/**
 * A <code>CellRequest</code> contains the context necessary to get a cell
 * value from a star.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
public class CellRequest {
	private RolapStar.Measure measure;
	private ArrayList columnList = new ArrayList();
	private ArrayList valueList = new ArrayList();

	/** Creates a {@link CellRequest}. **/
	public CellRequest(RolapStar.Measure measure) {
		this.measure = measure;
		this.columnList.add(measure);
	}

	public void addConstrainedColumn(RolapStar.Column column, Object[] values) {
		columnList.add(column);
		valueList.add(values);
	}

	public void addConstrainedColumn(RolapStar.Column column, Object value) {
		columnList.add(column);
		valueList.add(value);
	}

	void addColumn(RolapStar.Column column) {
		addConstrainedColumn(column, null);
	}

	public RolapStar.Measure getMeasure() {
		return measure;
	}

	public RolapStar.Column[] getColumns() {
		// ignore the measure, the 0th element of columnList
		RolapStar.Column[] a = new RolapStar.Column[columnList.size() - 1];
		for (int i = 0; i < a.length; i++) {
			a[i] = (RolapStar.Column) columnList.get(i + 1);
		}
		return a;
	}

	/** Returns a list which identifies which batch this request will
	 * belong to. The list contains the measure as well as the
	 * columns. **/
	public ArrayList getBatchKey() {
		return columnList;
	}

	public ArrayList getValueList() {
		return valueList;
	}

	public Object[] getSingleValues() {
		Object[] a = new Object[valueList.size()];
		for (int i = 0, n = valueList.size(); i < n; i++) {
			Object value = valueList.get(i);
			if (value instanceof Object[]) {
				throw Util.newInternal("multi value in cell request");
			}
			a[i] = value;
		}
		return a;
	}
}

// End CellRequest.java
