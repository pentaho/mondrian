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
import mondrian.resource.ChainableThrowable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * todo:
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public class Util extends mondrian.xom.XOMUtil
{
	public static final Object nullValue = new NullCellValue();
	private static Hashtable threadRes = new Hashtable();

	/** encodes string for MDX (escapes ] as ]] inside a name) */
	public static String mdxEncodeString(String st)
	{
		String retString = new String();
		for (int i = 0; i < st.length(); i++) {
			if (st.charAt(i) == ']' && (i+1) < st.length() 
				&& st.charAt(i+1) != '.')
				retString += "]"; //escaping character
			retString += st.charAt(i);
		}
		return retString;
	}

	/** Return quoted */
	public static String quoteForMdx(String val)
	{
		String s0 = replace(val, "\"", "\"\"");
		return "\"" + s0 + "\"";
	}

	/**
	 * Return string quoted in [...].  For example, "San Francisco" becomes
	 * "[San Francisco]".  todo: "a [bracketed] string" should become "[a
	 * [bracketed]] string]", but does not at present.
	 */
	public static String quoteMdxIdentifier(String id)
	{
		return "[" + id + "]";
	}

	/**
	 * Return identifiers quoted in [...].[...].  For example, {"Store", "USA",
	 * "California"} becomes "[Store].[USA].[California]".
	 **/
	public static String quoteMdxIdentifier(String[] ids)
	{
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < ids.length; i++) {
			if (i > 0) {
				sb.append(".");
			}
			sb.append(quoteMdxIdentifier(ids[i]));
		}
		return sb.toString();
	}

	/** Does not modify the original string */
	public static String replace(String s,String find,String replace)
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

	public static String[] explode(String s)
	{
		Vector vector = new Vector();
		int i = 0;
		while (i < s.length()) {
			if (s.charAt(i) != '[') {
				throw getRes().newMdxInvalidMember(s);
			}
			// s may contain extra ']' characters, so look for a ']' followed
			// by a '.' (still not perfect... really ought to scan, ignoring
			// escaped ']]' sequences)
			int j = s.indexOf("].", i);
			if (j == -1) {
				j = s.lastIndexOf("]");
			}
			if (j <= i) {
				throw getRes().newMdxInvalidMember(s);
			}
			String sub = s.substring(i + 1, j);
			vector.addElement(sub);
			if (j + 1 < s.length())
				if (s.charAt(j+1) != '.') {
					throw getRes().newMdxInvalidMember(s);
				}
			i = j +  2;
		}
		String[] names = new String[vector.size()];
		vector.copyInto(names);
		return names;
	}

	public static String implode(String[] names)
	{
		if (names.length == 0) {
			return "";
		}
		StringBuffer sb = new StringBuffer("[");
		for (int i = 0; i < names.length; i++) {
			if (i > 0) {
				sb.append("].[");
			}
			sb.append(names[i]);
		}
		sb.append("]");
		return sb.toString();
	}

	public static String makeFqName(String name)
	{
		return quoteMdxIdentifier(name);
	}

	public static String makeFqName(OlapElement parent, String name)
	{
		if (parent == null) {
			return Util.quoteMdxIdentifier(name);
		} else {
			return parent.getUniqueName() + "." + quoteMdxIdentifier(name);
		}
	}

	public static String makeFqName(String parentUniqueName, String name)
	{
		if (parentUniqueName == null) {
			return quoteMdxIdentifier(name);
		} else {
			return parentUniqueName + "." + quoteMdxIdentifier(name);
		}
	}

	/**
	 * Resolves a name such as '[Products].[Product Department].[Produce]' by
	 * parsing out the components ('Products', etc.) and resolving them one at
	 * a time.
	 */
	public static OlapElement lookupCompound(
		NameResolver st, String s, OlapElement mdxElement)
	{
		return lookupCompound(st, explode(s), mdxElement);
	}

	public static OlapElement lookupCompound(
		NameResolver st, String[] names, OlapElement mdxElement)
	{
		for (int i = 0; mdxElement != null && i < names.length; i++) {
			String sub = names[i];
			mdxElement = st.lookupChild(mdxElement, sub, false);
		}
		return mdxElement;
	}

	/**
	 * As {@link #lookupCompound(NameResolver,String,OlapElement)}, but with
	 * the option to fail instead of returning null.
	 */
	public static OlapElement lookupCompound(
		NameResolver st, String s, OlapElement mdxElement,
		boolean failIfNotFound)
	{
		OlapElement e = lookupCompound(st, s, mdxElement);
		if (e == null && failIfNotFound) {
			throw getRes().newMdxChildObjectNotFound(
				s, mdxElement.getQualifiedName());
		}
		return e;
	}

	public static Member lookupMemberCompound(
		NameResolver st, String[] names, boolean failIfNotFound)
	{
		OlapElement mdxElem = lookupCompound(st, names, st.getCube());
		if (mdxElem instanceof Member) {
			return (Member) mdxElem;
		} else if (failIfNotFound) {
			String s = implode(names);
			throw getRes().newMdxCantFindMember(s);
		}
		return null;
	}

	public static Member lookupMember(
		NameResolver st, String s, boolean failIfNotFound)
	{
		Member member = st.lookupMemberFromCache(s);
		if (member != null) {
			return member;
		}
		OlapElement mdxElem = lookupCompound(
			st, s, st.getCube(), failIfNotFound);
		if (mdxElem instanceof Member) {
			return (Member) mdxElem;
		} else if (failIfNotFound) {
			throw getRes().newMdxCantFindMember(s);
		}
		return null;
	}

	;

	public static class NullCellValue
	{
		public String toString()
		{
			return "#NULL";
		}
	};

	public static class ErrorCellValue
	{
		public String toString()
		{
			return "#ERR";
		}
	};

	/**
	 * Throws an internal error if condition is not true. It would be called
	 * <code>assert</code>, but that is a keyword as of JDK 1.4.
	 */
	public static void assertTrue(boolean b) {
		if (!b) {
			throw getRes().newInternal("assert failed");
		}
	}
	
	/**
	 * Throws an internal error with the given messagee if condition is not
	 * true. It would be called <code>assert</code>, but that is a keyword as
	 * of JDK 1.4.
	 */
	public static void assertTrue(boolean b, String message) {
		if (!b) {
			throw getRes().newInternal("assert failed: " + message);
		}
	}

	public static Error newInternal(String message) {
		return getRes().newInternal(message);
	}

	public static Error newInternal(Throwable e, String message) {
		return getRes().newInternal(e, message);
	}

	public static void setThreadRes(MondrianResource resource)
	{
		if (resource == null) {
			threadRes.remove(Thread.currentThread());
		} else {
			threadRes.put(Thread.currentThread(), resource);
		}
	}

	public static MondrianResource getRes()
	{
		MondrianResource resource = (MondrianResource) threadRes.get(
			Thread.currentThread());
		if (resource == null) {
			resource = MondrianResource.instance();
		}
		return resource;
	}

	/**
	 * Converts an error into an array of strings, the most recent error first.
	 *
	 * @param e the error; may be null. Errors are chained if the error
	 *    implmements {@link ChainableThrowable}.
	 **/
	public static String[] convertStackToString(Throwable e)
	{
		Vector v = new Vector();
		while (e != null) {
			String sMsg = getErrorMessage(e);
			v.addElement(sMsg);
			if (e instanceof ChainableThrowable) {
				e = ((ChainableThrowable) e).getNextThrowable();
			} else {
				e = null;
			}
		}
		String[] msgs = new String[v.size()];
		v.copyInto(msgs);
		return msgs;
	}

	/**
	 * @see #getErrorMessage(Throwable,boolean)
	 **/
	public static String getErrorMessage(Throwable err)
	{
		boolean prependClassName =
			!(err instanceof java.sql.SQLException ||
			  err.getClass() == java.lang.Exception.class);
		return getErrorMessage(err, prependClassName);
	}

	/**
	 * Constructs the message associated with an arbitrary Java error, making
	 * up one based on the stack trace if there is none.
	 *
	 * @param err the error
	 * @param prependClassName should the error be preceded by the
	 *   class name of the Java exception?  defaults to false, unless the error
	 *   is derived from {@link java.sql.SQLException} or is exactly a {@link
	 *   java.lang.Exception}
	 */
	public static String getErrorMessage(
		Throwable err, boolean prependClassName)
	{
		String errMsg = err.getMessage();
		if ((errMsg == null) || (err instanceof RuntimeException)) {
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			err.printStackTrace(pw);
			return sw.toString();
		} else {
			if (prependClassName) {
				return err.getClass().getName() + ": " + errMsg;
			} else {
				return errMsg;
			}

		}
	}

	/**
	 * <code>PropertyList</code> is an order-preserving list of key-value
	 * pairs. Lookup is case-insensitive, but the case of keys is preserved.
	 **/
	public static class PropertyList
	{
		Vector v = new Vector();

		public String get(String key)
		{
			for (int i = 0, n = v.size(); i < n; i++) {
				String[] pair = (String[]) v.elementAt(i);
				if (pair[0].equalsIgnoreCase(key)) {
					return pair[1];
				}
			}
			return null;
		}

		public String put(String key, String value)
		{
			for (int i = 0, n = v.size(); i < n; i++) {
				String[] pair = (String[]) v.elementAt(i);
				if (pair[0].equalsIgnoreCase(key)) {
					String old = pair[1];
					pair[1] = value;
					return old;
				}
			}
			v.addElement(new String[] {key, value});
			return null;
		}

		public String toString()
		{
			StringBuffer sb = new StringBuffer();
			for (int i = 0, n = v.size(); i < n; i++) {
				String[] pair = (String[]) v.elementAt(i);
				if (i++ > 0) {
					sb.append("; ");
				}
				sb.append(pair[0]);
				sb.append("=");
				sb.append(pair[1]);
			}
			return sb.toString();
		}
	};





	/**
	 * Converts an OLE DB connect string such as "Provider=MSOLAP;
	 * DataSource=LOCALHOST;" into a {@link PropertyList} containing (key,
	 * value) pairs {("Provider","MSOLAP"), ("DataSource", "LOCALHOST")}.
	 *
	 * <p>Syntax Notes (quotes are not implemented)<ul>
	 *
	 * <li> Values may be delimited by single or double quotes, (for example,
	 * name='value' or name="value"). Either single or double quotes may be
	 * used within a connection string by using the other delimiter, for
	 * example, name="value's" or name='value&quot;s',but not name='value's' or
	 * name=""value"". The value type is irrelevant.
	 *
	 * <li> All blank characters, except those placed within a value or within
	 * quotes, are ignored.
	 *
	 * <li> Keyword value pairs must be separated by a semicolon (;). If a
	 * semicolon is part of a value, it also must be delimited by quotes.
	 *
	 * <li> Names are not case sensitive. If a given name occurs more than once
	 * in the connection string, the value associated with the last occurence
	 * is used.
	 *
	 * <li> No escape sequences are supported. 
	 * </ul>
	 **/
	public static PropertyList parseConnectString(String s)
	{
		PropertyList properties = new PropertyList();
		StringTokenizer st = new StringTokenizer(s, ";");
		while (st.hasMoreTokens()) {
			String pair = st.nextToken(); // e.g. "Provider=MSOLAP"
			String key, value;
			int eq = pair.indexOf("=");
			if (eq < 0) {
				key = pair;
				value = null;
			} else {
				key = pair.substring(0, eq);
				value = pair.substring(eq + 1);
			}
			key = replace(key, " ", ""); // e.g. "Data Source" -> "DataSource"
			properties.put(key, value);
		}
		return properties;
	}

}

// End Util.java
