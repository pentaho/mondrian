/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2003 Kana Software, Inc. and others.
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

	void resolve(Exp.Resolver resolver) {
		exp = resolver.resolveChild(exp);
	}

	public Object[] getChildren()
	{
		return new Exp[] {exp};
	}

	public void replaceChild(int ordinal, QueryPart with) {
		Util.assertTrue(ordinal == 0);
		exp = (Exp) with;
	}

	public void unparse(PrintWriter pw) {
		pw.print(name + " = ");
		exp.unparse(pw);
	}

	/** Retrieves a property by name from an array. **/
	static Exp get(MemberProperty[] a, String name) {
		for (int i = 0; i < a.length; i++) {
			if (a[i].name.equals(name)) {
				return a[i].exp;
			}
		}
		return null;
	}
}


// End MemberProperty.java
