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
import java.util.Date;
import java.lang.reflect.Array;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;

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

	/**
	 * Executes a query, printing to the trace log if tracing is enabled.
	 * If the query fails, it throws the same {@link SQLException}, and closes
	 * the result set. If it succeeds, the caller must close the returned
	 * {@link ResultSet}.
	 */
	public static ResultSet executeQuery(
			Connection jdbcConnection, String sql, String component)
			throws SQLException {
		Statement statement = null;
		ResultSet resultSet = null;
		String status = "failed";
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.print(
				component + ": executing sql [" + sql + "]");
			RolapUtil.debugOut.flush();
		}
		try {
			statement = jdbcConnection.createStatement();
			Date date = new Date();
			resultSet = statement.executeQuery(sql);
			long time = (new Date().getTime() - date.getTime());
			status = ", " + time + " ms";
			return resultSet;
		} catch (SQLException e) {
			status = ", failed (" + e + ")";
			try {
				if (statement != null) {
					statement.close();
				}
			} catch (SQLException e2) {
				// ignore
			}
			throw (SQLException) e.fillInStackTrace();
		} finally {
			if (RolapUtil.debugOut != null) {
				RolapUtil.debugOut.println(status);
			}
		}
	}
}


// End RolapUtil.java
