/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;
import java.io.*;

public class QueryAxis extends QueryPart {
	public static final int subtotalsUndefined = -1;
	public static final int subtotalsHide = 0;
	public static final int subtotalsShow = 1;
	public static final EnumeratedValues subtotalsEnum = new EnumeratedValues(
		new String[] {"undefined", "hide", "show"},
		new int[] {-1, 0, 1});

	public boolean nonEmpty;
	public Exp set;
	public String axisName;
	public int axisOrdinal;
	/** <code>showSubtotals</code> indicates if "(show\hide)Subtotals"
	 * operation has been applied to axis*/
	private int  showSubtotals;
	
	public QueryAxis(
		boolean nonEmpty, Exp set, String axisName, int showSubtotals)
	{
		this.nonEmpty = nonEmpty;
		this.set = set;
		this.axisName = axisName;
		this.showSubtotals = showSubtotals;
	}

	public Object clone()
	{
		return new QueryAxis(nonEmpty, (Exp) set.clone(), axisName, showSubtotals);
	}

	static QueryAxis[] cloneArray(QueryAxis[] a)
	{
		QueryAxis[] a2 = new QueryAxis[a.length];
		for (int i = 0; i < a.length; i++)
			a2[i] = (QueryAxis) a[i].clone();
		return a2;
	}

	public QueryPart resolve(Query q)
	{
		set = (Exp) set.resolve(q);
		if (!set.isSet())
		{
			throw Util.getRes().newMdxAxisIsNotSet( axisName );
		}		
		return this;
	}

	public Object[] getChildren()
	{
		return new Object[] {set};
	}

	public void replaceChild(int ordinal, QueryPart with)
	{
		Util.assertTrue(ordinal == 0);
		set = (Exp) with;
	}

	public void unparse(PrintWriter pw, ElementCallback callback)
	{
		if (nonEmpty)
			pw.print("NON EMPTY ");
		if (set != null)
			set.unparse(pw, callback);

		// Plato can't handle missing axes, so for 'select {} on rows from
		// Sales', pretend that the 'rows' axis is really 'columns'.
		String name = callback.isPlatoMdx()
			? Query.axisNames[axisOrdinal] : this.axisName;
		pw.print(" ON " + name);
	}

	public void addLevel(Level level)
	{
		Util.assertTrue(level != null, "addLevel needs level");
		set = new FunCall(
			"Crossjoin",
			new Exp[] {
				set,
				new FunCall(
					"Members",
					new Exp[] {level},
					FunDef.TypeProperty)});
	}

	protected void setShowSubtotals(boolean bShowSubtotals)
	{
		if (bShowSubtotals)
			showSubtotals = subtotalsShow;
		else 
			showSubtotals = subtotalsHide;
	}

	public int getShowSubtotals()
	{return showSubtotals;}

	public void resetShowHideSubtotals()
	{this.showSubtotals = subtotalsUndefined;} 

	
}

// End QueryAxis.java
