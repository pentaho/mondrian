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
import java.util.Vector;

/**
 * Multi-part identifier.
 **/
public class Id
	extends ExpBase
	implements Cloneable
{
	private Vector names;
	private Vector keys;

	Id(String s, boolean key)
	{
		names = new Vector();
		keys = new Vector();
		names.addElement(s);
		keys.addElement(new Boolean(key));
	}

	Id(String s)
	{
		this(s, false);
	}

	private Id(Vector names, Vector keys)
	{
		this.names = names;
		this.keys = keys;
	}

	public Object clone()
	{
		return new Id((Vector) names.clone(), (Vector) keys.clone());
	}

	public int getType()
	{
		return Category.Unknown;
	}

	public boolean usesDimension(Dimension dimension)
	{
		// don't know til we resolve
		return false;
	}

	public String toString()
	{
		return Util.quoteMdxIdentifier(toStringArray());
	}

	public String[] toStringArray()
	{
		String[] ret = new String[names.size()];
		names.copyInto(ret);
		return ret;
	}

	public String getElement(int i)
	{
		return (String) names.elementAt(i);
	}

	public void append(String s, boolean key)
	{
		names.addElement(s);
		keys.addElement(new Boolean(key));
	}

	public void append(String s)
	{
		append(s, false);
	}

	public Exp resolve(Resolver resolver)
	{
		if (names.size() == 1) {
			final String s = (String) names.elementAt(0);
			if (FunTable.instance().isReserved(s)) {
				return Literal.createSymbol(s.toUpperCase());
			}
		}
		final String[] namesArray = toStringArray();
		return Util.lookup(resolver.getQuery(), namesArray);
	}

	public void unparse(PrintWriter pw)
	{
		for (int i = 0, n = names.size(); i < n; i++) {
			String s = (String) names.elementAt(i);
			if (i > 0) {
				pw.print(".");
			}
			if (((Boolean)keys.elementAt(i)).booleanValue()) {
				pw.print("&[" + Util.mdxEncodeString(s) + "]");
			} else {
				pw.print("[" + Util.mdxEncodeString(s) + "]");
			}
		}
	}

	// implement Exp
	public Object evaluate(Evaluator evaluator)
	{
		return evaluator.xx(this);
	}
}

// End Id.java
