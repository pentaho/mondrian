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
		return CatUnknown;
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
	
	public Exp resolve(Query q)
	{
		if (names.size() == 1) {
			String upper = ((String) names.elementAt(0)).toUpperCase();
			if (upper.equals("ASC") ||
					upper.equals("DESC") ||
					upper.equals("BASC") ||
					upper.equals("BDESC") ||
					upper.equals("ALL") ||
					upper.equals("RECURSIVE") ||
					upper.equals("SELF") ||
					upper.equals("AFTER") ||
					upper.equals("BEFORE") ||
					upper.equals("BEFORE_AND_AFTER") ||
					upper.equals("SELF_AND_AFTER") ||
					upper.equals("SELF_AND_BEFORE") ||
					upper.equals("SELF_BEFORE_AFTER") ||
					upper.equals("EXCLUDEEMPTY") ||
					upper.equals("INCLUDEEMPTY") ||
					upper.equals("PRE") ||
					upper.equals("POST") ||
					upper.equals("NULL") ||
					upper.equals("NUMERIC") ||
					upper.equals("STRING")) {
				return Literal.createSymbol(upper);
			}
		}
		// let's assume that this compound presents a unique mdx name and let's
		// try to look it up in a cube. if we fail we can lookup piece by piece
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		unparse(pw, new ElementCallback());
		String fullName = sw.toString();
		Member member = q.lookupMember(fullName, false);
		if (member != null) {
			return member;
		}
		// let's resolve it bit by bit
		OlapElement mdxElement = q.getCube();
		for (int i = 0, n = names.size(); i < n; i++) {
			String name = (String) names.elementAt(i);
			mdxElement = q.lookupChild(mdxElement, name, true);
		}
		return mdxElement;
	}

	public void unparse(PrintWriter pw, ElementCallback callBackObj)
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
