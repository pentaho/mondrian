/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2003 Kana Software, Inc. and others.
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
	private Object o;

	public static final Literal emptyString = new Literal("", false);
	public static final Literal zero = new Literal(new Integer(0));
	public static final Literal one = new Literal(new Integer(1));
	public static final Literal negativeOne = new Literal(new Integer(-1));
	public static final Literal doubleZero = new Literal(new Double(0.0));
	public static final Literal doubleOne = new Literal(new Double(1.0));
	public static final Literal doubleNegativeOne = new Literal(new Double(-1.0));

	private Literal(String s, boolean isSymbol)
	{
		this.o = s;
		this.type = isSymbol ? Category.Symbol : Category.String;
	}

	/**
	 * Creates a string literal.
	 * @see #createSymbol
	 */
	public static Literal createString(String s) {
		if (s.equals("")) {
			return emptyString;
		} else {
			return new Literal(s, false);
		}
	}
	/**
	 * Creates a symbol.
	 * @see #createString
	 */
	public static Literal createSymbol(String s) {
		return new Literal(s, true);
	}
	private Literal(Double d) {
		this.o = d;
		this.type = Category.Numeric;
	}
	public static Literal create(Double d) {
		if (d.doubleValue() == 0.0) {
			return doubleZero;
		} else if (d.doubleValue() == 1.0) {
			return doubleOne;
		} else {
			return new Literal(d);
		}
	}
	private Literal(Integer i) {
		this.o = i;
		this.type = Category.Numeric;
	}

	public static Literal create(Integer i) {
		if (i.intValue() == 0) {
			return zero;
		} else if (i.intValue() == 1) {
			return one;
		} else {
			return new Literal(i);
		}
	}

	public Object clone() {
		return this;
	}

	public void unparse(PrintWriter pw) {
		switch (type) {
		case Category.Symbol:
		case Category.Numeric:
			pw.print(o);
			break;
		case Category.String:
			pw.print(Util.quoteForMdx((String) o));
			break;
		default:
			throw Util.newInternal("bad literal type " + type);
		}
	}

	// from Exp
	public int getType() { return type; }
	public Hierarchy getHierarchy() { return null; }

	public Exp resolve(Resolver resolver) {
		return this;
	}

	public boolean usesDimension(Dimension dimension) {
		return false;
	}

	public Object evaluate(Evaluator evaluator) {
		return evaluator.xx(this);
	}

	public Object evaluateScalar(Evaluator evaluator) {
		return o;
	}

	public Object getValue() {
		return o;
	}

	public int getIntValue() {
		if (o instanceof Number) {
			return ((Number) o).intValue();
		} else {
			throw Util.newInternal("cannot convert " + o + " to int");
		}
	}
}

// End Literal.java
