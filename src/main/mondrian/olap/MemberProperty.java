/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 1 March, 2000
*/

package mondrian.olap;
import java.io.PrintWriter;

/**
 * Member property or solve order specification.
 **/
public class MemberProperty extends QueryPart {
	String name;
	Exp exp;

	MemberProperty(String name, Exp exp) {
		this.name = name;
		this.exp = exp;
	}

	protected Object clone()
	{
		return new MemberProperty(name, (Exp) exp.clone());
	}

	static MemberProperty[] cloneArray(MemberProperty[] x)
	{
		MemberProperty[] x2 = new MemberProperty[x.length];
		for (int i = 0; i < x.length; i++)
			x2[i] = (MemberProperty) x[i].clone();
		return x2;
	}

	public QueryPart resolve(Query q)
	{
		exp = exp.resolve(q);
		return this;
	}

	public Object[] getChildren()
	{
		return new Exp[] {exp};
	}

	public void replaceChild(int ordinal, QueryPart with)
	{
		Util.assertTrue(ordinal == 0);
		exp = (Exp) with;
	}

	public void unparse(PrintWriter pw, ElementCallback callback)
	{
		pw.print(name + " = ");
		if (exp instanceof Literal && ((Literal) exp).type == Category.String)
			pw.print("'");
		unparseValue(pw, callback);
		if (exp instanceof Literal && ((Literal) exp).type == Category.String)
			pw.print("'");
	}
	
	void unparseValue(PrintWriter pw, ElementCallback callback)
	{
		if (exp instanceof Literal &&
			((Literal) exp).type == Category.String) {
			exp.unparse(pw, callback);
		} else if (name.equalsIgnoreCase("SOLVE_ORDER")) {
			int i = ((Literal) exp).getIntValue();
			pw.print(i);
		} else {
			exp.unparse(pw, callback);
		}
	}

	/** Prints itself as xml element */
	public void printAsXml(PrintWriter pw)
	{
		pw.print("<MemberProperty ");
		Util.printAtt(pw, "name", name);
		pw.print(" value =\"");
		unparseValue(pw, new ElementCallback());
		pw.println("\" />");
	}

	/** Retrieves a property by name from an array. **/
	public static Exp get(MemberProperty[] a, String name)
	{
		for (int i = 0; i < a.length; i++) {
			if (a[i].name.equals(name)) {
				return a[i].exp;
			}
		}
		return null;
	}
}


// End MemberProperty.java
