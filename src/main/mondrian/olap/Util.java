/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

/**
 * Utility functions used throughout mondrian. All methods are static.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public class Util extends mondrian.xom.XOMUtil
{
	// properties

	public static final Object nullValue = new NullCellValue();

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
	 * "[San Francisco]"; "a [bracketed] string" becomes
     * "[a [bracketed]] string]".
	 */
	public static String quoteMdxIdentifier(String id) {
		return "[" + replace(id, "]", "]]") + "]";
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

	/** Returns true if two strings are equal, or are both null. **/
	public static boolean equals(String s, String t) {
		return s == null ?
			t == null :
			s.equals(t);
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
	 * Resolves a name such as
	 * '[Products]&#46;[Product Department]&#46;[Produce]' by resolving the
	 * components ('Products', and so forth) one at a time.
	 *
	 * @param schemaReader Schema reader, supplies access-control context
	 * @param parent Parent element to search in
	 * @param names Exploded compound name, such as {"Products",
	 *   "Product Department", "Produce"}
	 * @param failIfNotFound If the element is not found, determines whether
	 *   to return null or throw an error
	 * @param category Type of returned element, a {@link Category} value;
	 *   {@link Category#Unknown} if it doesn't matter.
	 * @pre parent != null
	 * @post !(failIfNotFound && return == null)
	 * @see #explode
	 */
	public static OlapElement lookupCompound(
			SchemaReader schemaReader, OlapElement parent, String[] names,
			boolean failIfNotFound, int category) {
		Util.assertPrecondition(parent != null, "parent != null");
		for (int i = 0; i < names.length; i++) {
			String name = names[i];
			final OlapElement child = schemaReader.getElementChild(parent, name);
			if (child == null) {
				if (failIfNotFound) {
					throw getRes().newMdxChildObjectNotFound(
						name, parent.getQualifiedName());
				} else {
					return null;
				}
			}
			parent = child;
		}
		switch (category) {
		case Category.Dimension:
			if (parent instanceof Dimension) {
				return parent;
			} else if (parent instanceof Hierarchy) {
				return parent.getDimension();
			} else if (failIfNotFound) {
				throw Util.newError("Can not find dimension '" + implode(names) + "'");
			} else {
				return null;
			}
		case Category.Hierarchy:
			if (parent instanceof Hierarchy) {
				return parent;
			} else if (parent instanceof Dimension) {
				return parent.getHierarchy();
			} else if (failIfNotFound) {
				throw Util.newError("Can not find hierarchy '" + implode(names) + "'");
			} else {
				return null;
			}
		case Category.Level:
			if (parent instanceof Level) {
				return parent;
			} else if (failIfNotFound) {
				throw Util.newError("Can not find level '" + implode(names) + "'");
			} else {
				return null;
			}
		case Category.Member:
			if (parent instanceof Member) {
				return parent;
			} else if (failIfNotFound) {
				throw getRes().newMdxCantFindMember(implode(names));
			} else {
				return null;
			}
		case Category.Unknown:
			assertPostcondition(parent != null, "return != null");
			return parent;
		default:
			throw newInternal("Bad switch " + category);
		}
	}

	/**
	 * Resolves a name such as
	 * '[Products]&#46;[Product Department]&#46;[Produce]'
	 * to a {@link Member} by parsing out the components
	 * {'Products', 'Product Department', 'Produce'}
	 * and resolving them one at a time.
	 *
	 * @pre st != null
	 * @pre cube != null
	 * @post !(failIfNotFound && return == null)
	 */
	public static Member lookupMemberCompound(
			SchemaReader st, Cube cube, String[] names, boolean failIfNotFound) {
		return (Member) lookupCompound(st, cube, names, failIfNotFound, Category.Member);
	}

	public static OlapElement lookup(Query q, String[] namesArray) {
		// First, look for a calculated member defined in the query.
		final String fullName = quoteMdxIdentifier(namesArray);
		OlapElement olapElement = q.lookupMemberFromCache(fullName);
		if (olapElement == null) {
			// Now look for any kind of object (member, level, hierarchy,
			// dimension) in the cube. Use a schema reader without restrictions.
//			final SchemaReader schemaReader = q.getSchemaReader();
			final SchemaReader schemaReader = q.getCube().getSchemaReader(null);
			olapElement = lookupCompound(schemaReader, q.getCube(), namesArray, false, Category.Unknown);
		}
		if (olapElement != null) {
			Role role = q.getConnection().getRole();
			if (!role.canAccess(olapElement)) {
				olapElement = null;
			}
		}
		if (olapElement == null) {
			throw Util.getRes().newMdxChildObjectNotFound(fullName, q.getCube().getQualifiedName());
		}
		return olapElement;
	}

	/**
	 * Finds a root member of a hierarchy with a given name.
	 *
	 * @param hierarchy
	 * @param memberName
	 * @return Member, or null if not found
	 */
	public static Member lookupHierarchyRootMember(
			SchemaReader reader, Hierarchy hierarchy, String memberName) {
		// Lookup member at first level.
		Member[] rootMembers = reader.getHierarchyRootMembers(hierarchy);
		for (int i = 0; i < rootMembers.length; i++) {
			if (rootMembers[i].getName().equalsIgnoreCase(memberName)) {
				return rootMembers[i];
			}
		}
		// If the first level is 'all', lookup member at second level. For
		// example, they could say '[USA]' instead of '[(All
		// Customers)].[USA]'.
		if (rootMembers.length == 1 && rootMembers[0].isAll()) {
			return lookupMemberChildByName(reader, rootMembers[0], memberName);
		}
		return null;
	}

	/**
	 * Finds a named level in this hierarchy. Returns null if there is no
	 * such level.
	 */
	public static Level lookupHierarchyLevel(Hierarchy hierarchy, String s) {
		final Level[] levels = hierarchy.getLevels();
		for (int i = 0; i < levels.length; i++) {
			if (levels[i].getName().equalsIgnoreCase(s)) {
				return levels[i];
			}
		}
		return null;
	}


	/**
	 * Finds a child of a member with a given name.
	 */
	public static Member lookupMemberChildByName(
			SchemaReader reader, Member member, String memberName) {
		Member[] children = reader.getMemberChildren(member);
		String childName = member.getUniqueName() + ".[" + memberName + "]";
		for (int i = 0; i < children.length; i++){
			if (childName.equals(children[i].getUniqueName()) ||
				childName.equals(removeCarriageReturn(children[i].getUniqueName()))) {
				return children[i];
			}
		}
		return null;
	}

	private static String removeCarriageReturn(String s) {
		return replace(s, "\r", "");
	}

	/**
	 * @param member
	 * @return
	 */
	public static int getMemberOrdinalInParent(SchemaReader reader, Member member) {
		Member parent = member.getParentMember();
		Member[] siblings;
		if (parent == null) {
			siblings = reader.getHierarchyRootMembers(member.getHierarchy());
		} else {
			siblings = reader.getMemberChildren(parent);
		}
		for (int i = 0; i < siblings.length; i++) {
			if (siblings[i] == member) {
				return i;
			}
		}
		throw Util.newInternal(
				"could not find member " + member + " amongst its siblings");
	}


	/**
	 * A <code>NullCellValue</code> is a placeholder value used when cells have
	 * a null value. It is a singleton.
	 */
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

	public static RuntimeException newInternal(String message) {
		return getRes().newInternal(message);
	}

	public static RuntimeException newInternal(Throwable e, String message) {
		return getRes().newInternal(message, e);
	}

	/** Creates a non-internal error. Currently implemented in terms of
	 * internal errors, but later we will create resourced messages. **/
	public static RuntimeException newError(String message) {
		return getRes().newInternal(message);
	}

	/** Creates a non-internal error. Currently implemented in terms of
	 * internal errors, but later we will create resourced messages. **/
	public static RuntimeException newError(Throwable e, String message) {
		return getRes().newInternal(message, e);
	}

	/**
	 * Checks that a precondition (declared using the javadoc <code>@pre</code>
	 * tag) is satisfied.
	 *
	 * @param b The value of executing the condition
	 */
	public static void assertPrecondition(boolean b) {
		assertTrue(b);
	}

	/**
	 * Checks that a precondition (declared using the javadoc <code>@pre</code>
	 * tag) is satisfied. For example,
	 *
	 * <blockquote><pre>void f(String s) {
	 *    Util.assertPrecondition(s != null, "s != null");
	 *    ...
	 * }</pre></blockquote>
	 *
	 * @param b The value of executing the condition
	 * @param condition The text of the condition
	 */
	public static void assertPrecondition(boolean b, String condition) {
		assertTrue(b, condition);
	}

	/**
	 * Checks that a postcondition (declared using the javadoc
	 * <code>@post</code> tag) is satisfied.
	 *
	 * @param b The value of executing the condition
	 */
	public static void assertPostcondition(boolean b) {
		assertTrue(b);
	}

	/**
	 * Checks that a postcondition (declared using the javadoc
	 * <code>@post</code> tag) is satisfied.
	 *
	 * @param b The value of executing the condition
	 */
	public static void assertPostcondition(boolean b, String condition) {
		assertTrue(b, condition);
	}

	public static MondrianResource getRes() {
		return MondrianResource.instance();
	}

    /**
     * Converts an error into an array of strings, the most recent error first.
     *
     * @param e the error; may be null. Errors are chained according to their
     *    {@link Throwable#getCause cause}.
     **/
    public static String[] convertStackToString(Throwable e)
    {
        Vector v = new Vector();
        while (e != null) {
            String sMsg = getErrorMessage(e);
            v.addElement(sMsg);
            e = e.getCause();
        }
        String[] msgs = new String[v.size()];
        v.copyInto(msgs);
        return msgs;
    }

	/**
     * Constructs the message associated with an arbitrary Java error, making
     * up one based on the stack trace if there is none. As
     * {@link #getErrorMessage(Throwable,boolean)}, but does not print the
     * class name if the exception is derived from {@link java.sql.SQLException}
     * or is exactly a {@link java.lang.Exception}.
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
	 * Creates a file-protocol URL for the given file.
	 **/
	public static URL toURL(File file) throws MalformedURLException {
		String path = file.getAbsolutePath();
		// This is a bunch of weird code that is required to
		// make a valid URL on the Windows platform, due
		// to inconsistencies in what getAbsolutePath returns.
		String fs = System.getProperty("file.separator");
		if (fs.length() == 1) {
			char sep = fs.charAt(0);
			if (sep != '/') {
				path = path.replace(sep, '/');
			}
			if (path.charAt(0) != '/') {
				path = '/' + path;
			}
		}
		path = "file://" + path;
		return new URL(path);
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
			if (key.equalsIgnoreCase("Provider")) {
				return "MSDASQL";
			}
			return null;
		}

		public String put(String key, String value)
		{
			for (int i = 0, n = v.size(); i < n; i++) {
				String[] pair = (String[]) v.elementAt(i);
				if (pair[0].equalsIgnoreCase(key)) {
					String old = pair[1];
					if (key.equalsIgnoreCase("Provider")) {
						// Unlike all other properties, later values of
						// "Provider" do not supersede
					} else {
						pair[1] = value;
					}
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
				if (i > 0) {
					sb.append("; ");
				}
				sb.append(pair[0]);
				sb.append("=");
				sb.append(pair[1]);
			}
			return sb.toString();
		}
	}

	/**
	 * Converts an OLE DB connect string into a {@link PropertyList}.
	 *
	 * <p> For example, <code>"Provider=MSOLAP; DataSource=LOCALHOST;"</code>
	 * becomes the set of (key, value) pairs <code>{("Provider","MSOLAP"),
	 * ("DataSource", "LOCALHOST")}</code>. Another example is
	 * <code>Provider='sqloledb';Data Source='MySqlServer';Initial
	 * Catalog='Pubs';Integrated Security='SSPI';</code>.
	 *
	 * <p> This method implements as much as possible of the <a
	 * href="http://msdn.microsoft.com/library/en-us/oledb/htm/oledbconnectionstringsyntax.asp"
	 * target="_blank">OLE DB connect string syntax
	 * specification</a>. To find what it <em>actually</em> does, take
	 * a look at the {@link mondrian.olap.Util.UtilTestCase JUnit test case}.
	 **/
	public static PropertyList parseConnectString(String s) {
		return new ConnectStringParser().parse(s);
	}

	private static class ConnectStringParser {
		String s;
		int i;
		int n;
		StringBuffer nameBuf = new StringBuffer();
		StringBuffer valueBuf = new StringBuffer();
		PropertyList parse(String s) {
			this.s = s;
			this.i = 0;
			this.n = s.length();
			PropertyList list = new PropertyList();
			while (i < n) {
				parsePair(list);
			}
			return list;
		}
		/**
		 * Reads "name=value;" or "name=value<EOF>".
		 */
		void parsePair(PropertyList list) {
			String name = parseName();
			String value;
			if (i >= n) {
				value = "";
			} else if (s.charAt(i) == ';') {
				i++;
				value = "";
			} else {
				value = parseValue();
			}
			list.put(name, value);
		}
		/**
		 * Reads "name=". Name can contain equals sign if equals sign is
		 * doubled.
		 */
		String parseName() {
			nameBuf.setLength(0);
			while (true) {
				char c = s.charAt(i);
				switch (c) {
				case '=':
					i++;
					if (i < n && (c = s.charAt(i)) == '=') {
						// doubled equals sign; take one of them, and carry on
						i++;
						nameBuf.append(c);
						break;
					}
                    String name = nameBuf.toString();
					name = name.trim();
					return name;
				case ' ':
					if (nameBuf.length() == 0) {
						// ignore preceding spaces
						i++;
						break;
					} else {
						// fall through
					}
				default:
					nameBuf.append(c);
					i++;
					if (i >= n) {
						return nameBuf.toString().trim();
					}
				}
			}
		}
		/**
		 * Reads "value;" or "value<EOF>"
		 */
		String parseValue() {
			char c;
			// skip over leading white space
			while ((c = s.charAt(i)) == ' ') {
				i++;
				if (i >= n) {
					return "";
				}
			}
			if (c == '"' || c == '\'') {
				String value = parseQuoted(c);
				// skip over trailing white space
				while (i < n && (c = s.charAt(i)) == ' ') {
					i++;
				}
				if (i >= n) {
					return value;
				} else if (s.charAt(i) == ';') {
					i++;
					return value;
				} else {
					throw new RuntimeException(
							"quoted value ended too soon, at position " + i +
							" in '" + s + "'");
				}
			} else {
				StringBuffer sbValue = new StringBuffer();
				// a jdbc url can contain semicolons (sql server: ...;SelectMethod=cursor)
				// in this case the semicolon must be escaped by a backslash
				int semi;
				while (true) {
					semi = s.indexOf(';', i);
					if (semi > 0 && s.charAt(semi -1) == '\\') {
						sbValue.append(s.substring(i, semi-1));
						sbValue.append(";");
						i = semi +1; // the semi is escaped
				    } else
				    	break;
			    }

				if (semi >= 0) {
					sbValue.append(s.substring(i, semi));
					i = semi + 1;
				} else {
					sbValue.append(s.substring(i));
					i = n;
				}
				return sbValue.toString().trim();
			}
		}
		/**
		 * Reads a string quoted by a given character. Occurrences of the
		 * quoting character must be doubled. For example,
		 * <code>parseQuoted('"')</code> reads <code>"a ""new"" string"</code>
		 * and returns <code>a "new" string</code>.
		 */
		String parseQuoted(char q) {
			char c = s.charAt(i++);
			Util.assertTrue(c == q);
			valueBuf.setLength(0);
			while (i < n) {
				c = s.charAt(i);
				if (c == q) {
					i++;
					if (i < n) {
						c = s.charAt(i);
						if (c == q) {
							valueBuf.append(c);
							i++;
							continue;
						}
					}
					return valueBuf.toString();
				} else {
					valueBuf.append(c);
					i++;
				}
			}
			throw new RuntimeException(
					"Connect string '" + s +
					"' contains unterminated quoted value '" +
					valueBuf.toString() + "'");
		}
	}

	/**
	 * Creates a JUnit testcase to test this class.
	 */
	public static Test suite() throws Exception {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(UtilTestCase.class);
		return suite;
	}

	/**
	 * Converts a list, which may or may not be mutable, into a mutable list.
	 * Non-mutable lists are returned by, for example,
	 * {@link List#subList}, {@link Arrays#asList},
	 * {@link Collections#unmodifiableList}.
	 */
	public static List makeMutable(List list) {
		if (list instanceof ArrayList) {
			return list;
		} else {
			return new ArrayList(list);
		}
	}

    /**
     * Creates a very simple implementation of {@link Exp.Resolver}. (Only
     * useful for resolving trivial expressions.)
     */
    public static Exp.Resolver createSimpleResolver(final FunTable funTable) {
        return new Exp.Resolver() {
            public Query getQuery() {
                throw new UnsupportedOperationException();
            }

            public Exp resolveChild(Exp exp) {
                return exp;
            }

            public Parameter resolveChild(Parameter parameter) {
                return parameter;
            }

            public void resolveChild(MemberProperty memberProperty) {
            }

            public void resolveChild(QueryAxis axis) {
            }

            public void resolveChild(Formula formula) {
            }

            public boolean requiresExpression() {
                return false;
            }

            public FunTable getFunTable() {
                return funTable;
            }

            public Parameter createOrLookupParam(FunCall call) {
                return null;
            }
        };
    }

    public static class UtilTestCase extends TestCase {
		public UtilTestCase(String s) {
			super(s);
		}
		public void testParseConnectStringSimple() {
			// Simple connect string
			PropertyList properties = parseConnectString("foo=x;bar=y;foo=z");
			assertEquals("y", properties.get("bar"));
			assertEquals("y", properties.get("BAR")); // get is case-insensitive
			assertNull(properties.get(" bar")); // get does not ignore spaces
			assertEquals("z", properties.get("foo")); // later occurrence overrides
			assertNull(properties.get("kipper"));
			assertEquals(2, properties.v.size());
			assertEquals("foo=z; bar=y", properties.toString());
		}
		public void testParseConnectStringComplex() {
			PropertyList properties = parseConnectString(
					"normalProp=value;" +
					"emptyValue=;" +
					" spaceBeforeProp=abc;" +
					" spaceBeforeAndAfterProp =def;" +
					" space in prop = foo bar ;" +
					"equalsInValue=foo=bar;" +
					"semiInProp;Name=value;" +
					" singleQuotedValue = 'single quoted value ending in space ' ;" +
					" doubleQuotedValue = \"=double quoted value preceded by equals\" ;" +
					" singleQuotedValueWithSemi = 'one; two';" +
					" singleQuotedValueWithSpecials = 'one; two \"three''four=five'");
			assertEquals(11, properties.v.size());
			String value;
			value = properties.get("normalProp");
			assertEquals("value", value);
			value = properties.get("emptyValue");
			assertEquals("", value); // empty string, not null!
			value = properties.get("spaceBeforeProp");
			assertEquals("abc", value);
			value = properties.get("spaceBeforeAndAfterProp");
			assertEquals("def", value);
			value = properties.get("space in prop");
			assertEquals(value, "foo bar");
			value = properties.get("equalsInValue");
			assertEquals("foo=bar", value);
			value = properties.get("semiInProp;Name");
			assertEquals("value", value);
			value = properties.get("singleQuotedValue");
			assertEquals("single quoted value ending in space ", value);
			value = properties.get("doubleQuotedValue");
			assertEquals("=double quoted value preceded by equals", value);
			value = properties.get("singleQuotedValueWithSemi");
			assertEquals(value, "one; two");
			value = properties.get("singleQuotedValueWithSpecials");
			assertEquals(value, "one; two \"three'four=five");
		}
		public void testConnectStringMore() {
			p("singleQuote=''''", "singleQuote", "'");
			p("doubleQuote=\"\"\"\"", "doubleQuote", "\"");
			p("empty= ;foo=bar", "empty", "");
		}
		/**
		 * Checks that <code>connectString</code> contains a property called
		 * <code>name</code>, whose value is <code>value</code>.
		 */
		void p(String connectString, String name, String expectedValue) {
			PropertyList list = parseConnectString(connectString);
			String value = list.get(name);
			assertEquals(expectedValue, value);
		}
		public void testOleDbSpec() {
			p("Provider='MSDASQL'", "Provider", "MSDASQL");
			p("Provider='MSDASQL.1'", "Provider", "MSDASQL.1");
			// If no Provider keyword is in the string, the OLE DB Provider for
			// ODBC (MSDASQL) is the default value. This provides backward
			// compatibility with ODBC connection strings. The ODBC connection
			// string in the following example can be passed in, and it will
			// successfully connect.
			p("Driver={SQL Server};Server={localhost};Trusted_Connection={yes};db={Northwind};", "Provider", "MSDASQL");
			// Specifying a Keyword
			//
			// To identify a keyword used after the Provider keyword, use the
			// property description of the OLE DB initialization property that you
			// want to set. For example, the property description of the standard
			// OLE DB initialization property DBPROP_INIT_LOCATION is
			// Location. Therefore, to include this property in a connection
			// string, use the keyword Location.
			p("Provider='MSDASQL';Location='3Northwind'", "Location", "3Northwind");
			// Keywords can contain any printable character except for the equal
			// sign (=).
			p("Jet OLE DB:System Database=c:\\system.mda", "Jet OLE DB:System Database", "c:\\system.mda");
			p("Authentication;Info=Column 5", "Authentication;Info", "Column 5");
			// If a keyword contains an equal sign (=), it must be preceded by an
			// additional equal sign to indicate that it is part of the keyword.
			p("Verification==Security=True", "Verification=Security", "True");
			// If multiple equal signs appear, each one must be preceded by an
			// additional equal sign.
			p("Many====One=Valid", "Many==One", "Valid");
			p("TooMany===False", "TooMany=", "False");
			// Setting Values That Use Reserved Characters
			//
			// To include values that contain a semicolon, single-quote character,
			// or double-quote character, the value must be enclosed in double
			// quotes.
			p("ExtendedProperties=\"Integrated Security='SSPI';Initial Catalog='Northwind'\"", "ExtendedProperties", "Integrated Security='SSPI';Initial Catalog='Northwind'");
			// If the value contains both a semicolon and a double-quote character,
			// the value can be enclosed in single quotes.
			p("ExtendedProperties='Integrated Security=\"SSPI\";Databse=\"My Northwind DB\"'", "ExtendedProperties", "Integrated Security=\"SSPI\";Databse=\"My Northwind DB\"");
			// The single quote is also useful if the value begins with a
			// double-quote character.
			p("DataSchema='\"MyCustTable\"'", "DataSchema", "\"MyCustTable\"");
			// Conversely, the double quote can be used if the value begins with a
			// single quote.
			p("DataSchema=\"'MyOtherCustTable'\"", "DataSchema", "'MyOtherCustTable'");
			// If the value contains both single-quote and double-quote characters,
			// the quote character used to enclose the value must be doubled each
			// time it occurs within the value.
			p("NewRecordsCaption='\"Company''s \"new\" customer\"'", "NewRecordsCaption", "\"Company's \"new\" customer\"");
			p("NewRecordsCaption=\"\"\"Company's \"\"new\"\" customer\"\"\"", "NewRecordsCaption", "\"Company's \"new\" customer\"");
			// Setting Values That Use Spaces
			//
			// Any leading or trailing spaces around a keyword or value are
			// ignored. However, spaces within a keyword or value are allowed and
			// recognized.
			p("MyKeyword=My Value", "MyKeyword", "My Value");
			p("MyKeyword= My Value ;MyNextValue=Value", "MyKeyword", "My Value");
			// To include preceding or trailing spaces in the value, the value must
			// be enclosed in either single quotes or double quotes.
			p("MyKeyword=' My Value  '", "MyKeyword", " My Value  ");
			p("MyKeyword=\"  My Value \"", "MyKeyword", "  My Value ");
			if (false) {
				// (Not supported.)
				//
				// If the keyword does not correspond to a standard OLE DB
				// initialization property (in which case the keyword value is
				// placed in the Extended Properties (DBPROP_INIT_PROVIDERSTRING)
				// property), the spaces around the value will be included in the
				// value even though quote marks are not used. This is to support
				// backward compatibility for ODBC connection strings. Trailing
				// spaces after keywords might also be preserved.
			}
			if (false) {
				// (Not supported)
				//
				// Returning Multiple Values
				//
				// For standard OLE DB initialization properties that can return
				// multiple values, such as the Mode property, each value returned
				// is separated with a pipe (|) character. The pipe character can
				// have spaces around it or not.
				//
				// Example   Mode=Deny Write|Deny Read
			}
			// Listing Keywords Multiple Times
			//
			// If a specific keyword in a keyword=value pair occurs multiple times
			// in a connection string, the last occurrence listed is used in the
			// value set.
			p("Provider='MSDASQL';Location='Northwind';Cache Authentication='True';Prompt='Complete';Location='Customers'", "Location", "Customers");
			// One exception to the preceding rule is the Provider keyword. If this
			// keyword occurs multiple times in the string, the first occurrence is
			// used.
			p("Provider='MSDASQL';Location='Northwind'; Provider='SQLOLEDB'", "Provider", "MSDASQL");
			if (false) {
				// (Not supported)
				//
				// Setting the Window Handle Property
				//
				// To set the Window Handle (DBPROP_INIT_HWND) property in a
				// connection string, a long integer value is typically used.
			}
		}
		public void testQuoteMdxIdentifier() {
			assertEquals("[San Francisco]", quoteMdxIdentifier("San Francisco"));
			assertEquals("[a [bracketed]] string]", quoteMdxIdentifier("a [bracketed] string"));
			assertEquals("[Store].[USA].[California]", quoteMdxIdentifier(new String[] {"Store", "USA", "California"}));
		}
	}
}

// End Util.java
