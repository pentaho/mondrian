/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 23 January, 1999
*/

package mondrian.olap;
import java.util.*;
import java.io.*;

/**
 * Component of an MDX query (derived classes include Query, Axis, Exp, Level).
 **/
public abstract class QueryPart extends Object implements Walkable
{
	protected QueryPart next;

	QueryPart()
	{
		next = null;
	}

	static QueryPart[] cloneArray(QueryPart[] a)
		throws CloneNotSupportedException
	{
		QueryPart[] a2 = new QueryPart[a.length];
		for (int i = 0; i < a.length; i++)
			a2[i] = (QueryPart) a[i].clone();
		return a2;
	}

	public void append(QueryPart next)
	{
		this.next = next;
	}

	public final int getChainLength()
	{
		int i = 1;
		for (QueryPart e = this; e.next != null; e = e.next)
			i++;
		return i;
	}

	public static QueryPart[] makeArray(QueryPart x)
	{
		QueryPart[] array = new QueryPart[x == null ? 0 : x.getChainLength()];
		for (int i = 0; x != null; x = x.next)
			array[i++] = x;
		return array;
	}

	public void unparse(PrintWriter pw, ElementCallback callback)
	{
		String toString = null;
		if (this instanceof OlapElement) {
			toString = callback.registerItself((OlapElement)this);
		}
		if (toString == null) {
			toString = toString();
		}
		if (callback.isPlatoMdx() &&
			this instanceof Member &&
			callback.findHiddenName(toString) != null) {
			toString = callback.findHiddenName(toString);
		}
		pw.print(toString);
	}

	protected String toStringHelper()
	{
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		ElementCallback callback = null;
		unparse(pw, callback);
		return sw.toString();
	}

	/** Replace the <code>ordinal</code>th child (as it appeared in the array
	 * returned from <code>getChildren()</code>) with <code>with</code>. */
	public void replaceChild(int ordinal, QueryPart with)
	{
		// By default, a QueryPart is atomic (has no children).
		throw new Error("unsupported");
	}

	// implement Walkable
	public Object[] getChildren()
	{
		// By default, a QueryPart is atomic (has no children).
		return null;
	}

	protected Object[] getAllowedChildren( CubeAccess cubeAccess )
	{
		// By default, a QueryPart is atomic (has no children).
		return null;
	}


	// -- Helper methods relating to name-resolution. -------------------------

}

// End QueryPart.java
