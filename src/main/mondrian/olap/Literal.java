/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 January, 1999
*/

package mondrian.olap;
import java.io.*;

/**
 * Constant (just strings and symbols for now).
 **/
public class Literal extends ExpBase
{
	public int type;
	public String s;			// just string literals for now

	public static final Literal emptyString = new Literal("", false);
	public static final Literal zero = new Literal(new Integer(0));
	public static final Literal one = new Literal(new Integer(1));
	public static final Literal doubleZero = new Literal(new Double(0.0));
	public static final Literal doubleOne = new Literal(new Double(1.0));

	private Literal(String s, boolean isSymbol)
	{
		this.s = s;
		this.type = isSymbol ? CatSymbol : CatString;
	}

	static Literal createString(String s) {
		if (s.equals("")) {
			return emptyString;
		} else {
			return new Literal(s, false);
		}
	}
	static Literal createSymbol(String s) {
		return new Literal(s, true);
	}
	private Literal(Double d) {
		this.s = d.toString();
		this.type = CatNumeric;
	}
	static Literal create(Double d) {
		if (d.doubleValue() == 0.0) {
			return doubleZero;
		} else if (d.doubleValue() == 1.0) {
			return doubleOne;
		} else {
			return new Literal(d);
		}
	}
	private Literal(Integer i) {
		this.s = i.toString();
		this.type = CatNumeric;
	}

	static Literal create(Integer i) {
		if (i.intValue() == 0) {
			return zero;
		} else if (i.intValue() == 1) {
			return one;
		} else {
			return new Literal(i);
		}
	}

	public Object clone()
	{
		return this;
	}

	public void unparse(PrintWriter pw, ElementCallback callback)
	{
		if (type == CatSymbol || type == CatNumeric) {
			pw.print( s );
		} else {
			pw.print(Util.quoteForMdx(s));
		}
	}

	// from Exp
	public int getType() { return type; }
	public Hierarchy getHierarchy() { return null; }

	public Exp resolve(Query q)
	{
		return this;
	}

	public boolean usesDimension(Dimension dimension)
	{
		return false;
	}

	// implement Exp
	public Object evaluate(Evaluator evaluator)
	{
		return evaluator.xx(this);
	}
}

// End Literal.java
