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
import java.io.PrintWriter;

/**
 * Component of an MDX query (derived classes include Query, Axis, Exp, Level).
 **/
public abstract class QueryPart implements Walkable
{
	QueryPart() {
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
}

// End QueryPart.java
