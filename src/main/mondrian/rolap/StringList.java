/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.Util;

/**
 * <code>StringList</code> makes it easy to build up a comma-separated string.
 *
 * @author jhyde
 * @since 29 December, 2001
 * @version $Id$
 **/
class StringList
{
	StringBuffer sb;
	String first, mid, last;
	int count;

	StringList(String first, String mid)
	{
		this.sb = new StringBuffer(first);
		this.count = 0;
		this.first = first;
		this.mid = mid;
		this.last = "";
	}
	StringList(String first)
	{
		this(first, ", ");
	}
	int getCount()
	{
		return count;
	}
	boolean isEmpty()
	{
		return count == 0;
	}
	/** Creates a new item. **/
	void newItem(String s)
	{
		if (count++ > 0) {
			sb.append(mid);
		}
		sb.append(s);
	}
	/** Appends to an existing item. **/
	void append(String s)
	{
		Util.assertTrue(count > 0);
		sb.append(s);
	}
	// override Object
	public String toString()
	{
		sb.append(last);
		return sb.toString();
	}
};


// End StringList.java
