/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;
import java.io.PrintWriter;

/**
 * <code>OlapElementBase</code> is an abstract base class for implementations of
 * {@link OlapElement}.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class OlapElementBase
	extends ExpBase
	implements OlapElement
{
	public boolean equals(Object o)
	{
		return (o instanceof OlapElement) &&
			equals((OlapElement) o);
	}

	public boolean equals(OlapElement mdxElement)
	{
		return getClass() == mdxElement.getClass() &&
			getUniqueName().equalsIgnoreCase(mdxElement.getUniqueName());
	}

	public int hashCode()
	{
		int i = (getClass().hashCode() << 8),
			j = stringHash(getUniqueName()),
			k = i ^ j;
		return k;
	}

	/** JDK1.1's string hashing algorithm only samples if the string is 16 or
	 * longer.  Member names are so similar that we want to read all
	 * characters. */
	public static final int stringHash(String s)
	{
		int h = 0;
		int len = s.length();

	    for (int i = 0; i < len; i++) {
			h = (h * 37) + s.charAt(i);
	    }
		return h;
    }
	
//  	public String getType()
//  	{
//  		// Take the class-name (e.g. Broadbase.mdx.Hierarchy) and remove
//  		// up to the last '.'.
//  		String s = getClass().getName();
//  		int i = s.lastIndexOf(".");
//  		if (i != -1)
//  			s = s.substring(i + 1);
//  		return s;
//  	}

	public String toString()
		{ return getUniqueName(); }

	private boolean isDescendantOf(OlapElement seek)
	{
		for (OlapElement e = this; e != null; e = e.getParent()) {
			if (e == seek) {
				return true;
			}
		}
		return false;
	}

	// implement Exp
	public boolean usesDimension(Dimension dim)
	{
		return isDescendantOf(dim);
	}

	public Object evaluate(Evaluator evaluator)
	{
		return evaluator.xx(this);
	}

	public Exp resolve(Query q)
	{
		return this;
	}

	// implement ExpBase
	public Object clone()
	{
		return this;
	}
}


// End OlapElementBase.java
