/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.Util;

import java.util.ArrayList;
import java.util.Vector;
import java.lang.reflect.Array;
import java.io.PrintWriter;

/**
 * todo:
 *
 * @author jhyde
 * @since 22 December, 2001
 * @version $Id$
 **/
public class RolapUtil {
	private static final RolapMember[] emptyMemberArray = new RolapMember[0];
	public static PrintWriter debugOut;

	/**
	 * Encloses a value in single-quotes, to make a SQL string value. Examples:
	 * <code>singleQuoteForSql(null)</code> yields <code>NULL</code>;
	 * <code>singleQuoteForSql("don't")</code> yields <code>'don''t'</code>.
	 **/
	static String singleQuoteForSql(String val)
	{
		if (val == null) {
			return "NULL";
		}
		String s0 = replace(val, "'", "''");
		return "'" + s0 + "'";
	}

	/**
	 * Returns <code>s</code> with <code>find</code> replaced everywhere with
	 * <code>replace</code>.
	 **/
	static String replace(String s,String find,String replace)
	{
		// let's be optimistic
		int found = s.indexOf(find);
		if (found == -1) {
			return s;
		}
		StringBuffer sb = new StringBuffer(s.length());
		int start = 0;
		for (;;) {
			for (; start < found; start++) {
				sb.append(s.charAt(start));
			}
			if (found == s.length()) {
				break;
			}
			sb.append(replace);
			start += find.length();
			found = s.indexOf(find,start);
			if (found == -1) {
				found = s.length();
			}
		}
		return sb.toString();
	}

	static final void add(ArrayList list, Object[] array)
	{
		for (int i = 0; i < array.length; i++) {
			list.add(array[i]);
		}
	}

	static final RolapMember[] toArray(Vector v)
	{
		RolapMember[] members = new RolapMember[v.size()];
		v.copyInto(members);
		return members;
	}

	static final RolapMember[] toArray(ArrayList v)
	{
		return (RolapMember[]) v.toArray(emptyMemberArray);
	}

	/**
	 * Adds an object to the end of an array.  The resulting array is of the
	 * same type (e.g. <code>String[]</code>) as the input array.
	 **/
	static Object[] addElement(Object[] a, Object o)
	{
		Class clazz = a.getClass().getComponentType();
		Object[] a2 = (Object[]) Array.newInstance(clazz, a.length + 1);
		System.arraycopy(a, 0, a2, 0, a.length);
		a2[a.length] = o;
		return a2;
	}

}


// End RolapUtil.java
