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

package mondrian.rolap;

import mondrian.olap.Util;

import java.util.Vector;

/**
 * A <code>CellRequest</code> contains the context necessary to get a cell value from a star.
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
public class CellRequest
{
	private RolapStar.Measure measure;
	private HashableVector columnsVector = new HashableVector();
	private Vector valuesVector = new Vector();
	CellRequest(RolapStar.Measure measure)
	{
		this.measure = measure;
		this.columnsVector.addElement(measure);
	}
	void addConstrainedColumn(RolapStar.Column column, Object[] values)
	{
		columnsVector.addElement(column);
		valuesVector.addElement(values);
	}
	void addConstrainedColumn(RolapStar.Column column, Object value)
	{
		columnsVector.addElement(column);
		valuesVector.addElement(value);
	}
	void addColumn(RolapStar.Column column)
	{
		addConstrainedColumn(column, null);
	}
	public RolapStar.Measure getMeasure()
	{
		return measure;
	}
	public RolapStar.Column[] getColumns() {
		// ignore the measure, the 0th element of columnsVector
		RolapStar.Column[] a = new RolapStar.Column[columnsVector.size() - 1];
		for (int i = 0; i < a.length; i++) {
			a[i] = (RolapStar.Column) columnsVector.elementAt(i + 1);
		}
		return a;
	}
	/** Returns a vector which identifies which batch this request will
	 * belong to. The vector contains the measure as well as the
	 * columns. **/
	HashableVector getBatchKey() {
		return columnsVector;
	}
	public Vector getValuesVector() {
		return valuesVector;
	}
/*
		Object[][] getMultiValues() {
			Object[][] a = new Object[valuesVector.size()][];
			valuesVector.copyInto(a);
			return a;
		}
*/
	public Object[] getSingleValues() {
		Object[] a = new Object[valuesVector.size()];
		for (int i = 0, n = valuesVector.size(); i < n; i++) {
			Object value = valuesVector.elementAt(i);
			if (value instanceof Object[]) {
				throw Util.newInternal("multi value in cell request");
			}
			a[i] = value;
		}
		return a;
	}
}
