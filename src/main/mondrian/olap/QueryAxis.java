/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;
import java.io.*;

public class QueryAxis extends QueryPart {
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

	public void resolve(Exp.Resolver resolver)
	{
		set = resolver.resolveChild(set);
		if (!set.isSet()) {
			throw Util.getRes().newMdxAxisIsNotSet( axisName );
		}		
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

	public void unparse(PrintWriter pw)
	{
		if (nonEmpty) {
			pw.print("NON EMPTY ");
        }
		if (set != null) {
			set.unparse(pw);
        }
        pw.print(" ON " + axisName);
	}

	public void addLevel(Level level)
	{
		Util.assertTrue(level != null, "addLevel needs level");
        set = new FunCall("Crossjoin", Syntax.Function, new Exp[] {
            set,
            new FunCall("Members", Syntax.Property, new Exp[] {level})
        });
	}

	void setShowSubtotals(boolean bShowSubtotals) {
		showSubtotals = bShowSubtotals ? SubtotalVisibility.Show : SubtotalVisibility.Hide;
	}

	public int getShowSubtotals() {
		return showSubtotals;
	}

	public void resetShowHideSubtotals() {
		this.showSubtotals = SubtotalVisibility.Undefined;
	}

	/**
	 * <code>SubtotalVisibility</code> enumerates the allowed values of
	 * whether subtotals are visible.
	 **/
	public static class SubtotalVisibility extends EnumeratedValues {
		/** The singleton instance of <code>SubtotalVisibility</code>. **/
		public static final SubtotalVisibility instance = new SubtotalVisibility();

		private SubtotalVisibility() {
			super(new String[] {"undefined", "hide", "show"},
					new int[] {Undefined, Hide, Show});
		}
		public static final int Undefined = -1;
		public static final int Hide = 0;
		public static final int Show = 1;
	}
}

// End QueryAxis.java
