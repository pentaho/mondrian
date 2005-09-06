/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import org.apache.log4j.Logger;
import org.eigenbase.xom.XOMUtil;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import mondrian.olap.fun.FunUtil;

/**
 * Utility functions used throughout mondrian. All methods are static.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public class Util extends XOMUtil {

    public static final String nl = System.getProperty("line.separator");

    private static final Logger LOGGER = Logger.getLogger(Util.class);
    // properties

    public static final Object nullValue = new NullCellValue();

    /**
     * Cumulative time spent accessing the database.
     */
    private static long databaseMillis = 0;

    /**
     * Random number generator to provide seed for other random number
     * generators.
     */
    private static final Random metaRandom =
            createRandom(MondrianProperties.instance().TestSeed.get());

    /** encodes string for MDX (escapes ] as ]] inside a name) */
    public static String mdxEncodeString(String st) {
        StringBuffer retString = new StringBuffer(st.length() + 20);
        for (int i = 0; i < st.length(); i++) {
            char c = st.charAt(i);
            if ((c == ']') &&
                ((i+1) < st.length()) &&
                (st.charAt(i+1) != '.')) {

                retString.append(']'); //escaping character
            }
            retString.append(c);
        }
        return retString.toString();
    }


    /**
     * Converts a string into a double-quoted string.
     */
    public static String quoteForMdx(String val) {
        StringBuffer buf = new StringBuffer(val.length()+20);
        buf.append("\"");

        String s0 = replace(val, "\"", "\"\"");
        buf.append(s0);

        buf.append("\"");
        return buf.toString();
    }

    /**
     * Return string quoted in [...].  For example, "San Francisco" becomes
     * "[San Francisco]"; "a [bracketed] string" becomes
     * "[a [bracketed]] string]".
     */
    public static String quoteMdxIdentifier(String id) {
        StringBuffer buf = new StringBuffer(id.length() + 20);
        quoteMdxIdentifier(id, buf);
        return buf.toString();
    }

    public static void quoteMdxIdentifier(String id, StringBuffer buf) {
        buf.append('[');
        int start = buf.length();
        buf.append(id);
        replace(buf, start, "]", "]]");
        buf.append(']');
    }

    /**
     * Return identifiers quoted in [...].[...].  For example, {"Store", "USA",
     * "California"} becomes "[Store].[USA].[California]".
     **/
    public static String quoteMdxIdentifier(String[] ids) {
        StringBuffer sb = new StringBuffer(64);
        quoteMdxIdentifier(ids, sb);
        return sb.toString();
    }

    public static void quoteMdxIdentifier(String[] ids, StringBuffer sb) {
        for (int i = 0; i < ids.length; i++) {
            if (i > 0) {
                sb.append('.');
            }
            quoteMdxIdentifier(ids[i], sb);
        }
    }

    /**
     * Returns true if two objects are equal, or are both null.
     */
    public static boolean equals(Object s, Object t) {
        return (s == null) ? (t == null) : s.equals(t);
    }

    /**
     * Returns true if two strings are equal, or are both null.
     * Takes into account the {@link MondrianProperties#CaseSensitive case
     * sensitive option}.
     */
    public static boolean equals(String s, String t) {
        boolean caseSensitive = MondrianProperties.instance().CaseSensitive.get();
        return (s == null) ?
                (t == null) :
                (caseSensitive ? s.equals(t) : s.equalsIgnoreCase(t));
    }

    /**
     * Returns a string with every occurrence of a seek string replaced with
     * another.
     */
    public static String replace(String s, String find, String replace) {
        // let's be optimistic
        int found = s.indexOf(find);
        if (found == -1) {
            return s;
        }
        StringBuffer sb = new StringBuffer(s.length() + 20);
        int start = 0;
        char[] chars = s.toCharArray();
        final int step = find.length();
        if (step == 0) {
            // Special case where find is "".
            sb.append(s);
            replace(sb, 0, find, replace);
        } else {
            for (;;) {
                sb.append(chars, start, found-start);
                if (found == s.length()) {
                    break;
                }
                sb.append(replace);
                start = found + step;
                found = s.indexOf(find, start);
                if (found == -1) {
                    found = s.length();
                }
            }
        }
        return sb.toString();
    }

    /**
     * Replaces all occurrences of a string in a buffer with another.
     *
     * @param buf String buffer to act on
     * @param start
     * @param find String to find
     * @param replace String to replace it with
     * @return The string buffer
     */
    public static StringBuffer replace(
            StringBuffer buf,
            int start,
            String find, String replace) {

        // Search and replace from the end towards the start, to avoid O(n ^ 2)
        // copying if the string occurs very commonly.
        int findLength = find.length();
        if (findLength == 0) {
            // Special case where the seek string is empty.
            for (int j = buf.length(); j >= 0; --j) {
                buf.insert(j, replace);
            }
            return buf;
        }
        int k = buf.length();
        while (k > 0) {
            int i = buf.lastIndexOf(find, k);
            if (i < start) {
                break;
            }
            buf.replace(i, i + find.length(), replace);
            // Step back far enough to ensure that the beginning of the section
            // we just replaced does not cause a match.
            k = i - findLength;
        }
        return buf;
    }

    public static String[] explode(String s) {
        List list = new ArrayList();
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
                j = s.lastIndexOf(']');
            }
            if (j <= i) {
                throw getRes().newMdxInvalidMember(s);
            }
            String sub = s.substring(i + 1, j);
            list.add(sub);
            if (j + 1 < s.length())
                if (s.charAt(j+1) != '.') {
                    throw getRes().newMdxInvalidMember(s);
                }
            i = j +  2;
        }
        String[] names = (String[]) list.toArray(new String[list.size()]);
        return names;
    }

    public static String implode(String[] names) {
        if (names.length == 0) {
            return "";
        }
        StringBuffer sb = new StringBuffer(64);
        sb.append('[');
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                sb.append("].[");
            }
            sb.append(names[i]);
        }
        sb.append(']');
        return sb.toString();
    }

    public static String makeFqName(String name) {
        return quoteMdxIdentifier(name);
    }

    public static String makeFqName(OlapElement parent, String name) {
        if (parent == null) {
            return Util.quoteMdxIdentifier(name);
        } else {
            StringBuffer buf = new StringBuffer(64);
            buf.append(parent.getUniqueName());
            buf.append('.');
            Util.quoteMdxIdentifier(name, buf);
            return buf.toString();
        }
    }

    public static String makeFqName(String parentUniqueName, String name) {
        if (parentUniqueName == null) {
            return quoteMdxIdentifier(name);
        } else {
            StringBuffer buf = new StringBuffer(64);
            buf.append(parentUniqueName);
            buf.append('.');
            Util.quoteMdxIdentifier(name, buf);
            return buf.toString();
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
        SchemaReader schemaReader,
        OlapElement parent,
        String[] names,
        boolean failIfNotFound,
        int category) {

        Util.assertPrecondition(parent != null, "parent != null");

        if (LOGGER.isDebugEnabled()) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("Util.lookupCompound: ");
            buf.append("parent.name=");
            buf.append(parent.getName());
            buf.append(", category=");
            buf.append(Category.instance.getName(category));
            buf.append(", names=");
            quoteMdxIdentifier(names, buf);
            LOGGER.debug(buf.toString());
        }

        // First look up a member from the cache of calculated members
        // (cubes and queries both have them).
        switch (category) {
        case Category.Member:
        case Category.Unknown:
            Member member = schemaReader.getCalculatedMember(names);
            if (member != null) {
                return member;
            }
        }
        // Likewise named set.
        switch (category) {
        case Category.Set:
        case Category.Unknown:
            NamedSet namedSet = schemaReader.getNamedSet(names);
            if (namedSet != null) {
                return namedSet;
            }
        }

        // Now resolve the name one part at a time.
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            final OlapElement child = schemaReader.getElementChild(parent, name);
            if (child == null) {
                if (LOGGER.isDebugEnabled()) {
                    StringBuffer buf = new StringBuffer(64);
                    buf.append("Util.lookupCompound: ");
                    buf.append("parent.name=");
                    buf.append(parent.getName());
                    buf.append(" has no child with name=");
                    buf.append(name);
                    LOGGER.debug(buf.toString());
                }

                if (failIfNotFound) {
                    throw getRes().newMdxChildObjectNotFound(
                        name, parent.getQualifiedName());
                } else {
                    return null;
                }
            }
            parent = child;
        }
        if (LOGGER.isDebugEnabled()) {
            StringBuffer buf = new StringBuffer(64);
            buf.append("Util.lookupCompound: ");
            buf.append("found child.name=");
            buf.append(parent.getName());
            buf.append(", child.class=");
            buf.append(parent.getClass().getName());
            LOGGER.debug(buf.toString());
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

    public static OlapElement lookup(Query q, String[] nameParts) {

        // First, look for a calculated member defined in the query.
        final String fullName = quoteMdxIdentifier(nameParts);
        // Look for any kind of object (member, level, hierarchy,
        // dimension) in the cube. Use a schema reader without restrictions.
        final SchemaReader schemaReader = q.getSchemaReader(false);
        OlapElement olapElement = schemaReader.lookupCompound(
            q.getCube(), nameParts, false, Category.Unknown);
        if (olapElement != null) {
            Role role = q.getConnection().getRole();
            if (!role.canAccess(olapElement)) {
                olapElement = null;
            }
        }
        if (olapElement == null) {
            throw Util.getRes().newMdxChildObjectNotFound(
                    fullName, q.getCube().getQualifiedName());
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
    public static Member lookupHierarchyRootMember(SchemaReader reader,
                                                   Hierarchy hierarchy,
                                                   String memberName) {
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
        return (rootMembers.length == 1 && rootMembers[0].isAll())
            ? lookupMemberChildByName(reader, rootMembers[0], memberName)
            : null;
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
        // TODO: Linear search may be a performance problem.
        for (int i = 0; i < children.length; i++){
            final Member child = children[i];
            if (Util.equals(child.getName(), memberName)) {
                return child;
            }
        }
        return null;
    }

    /**
     * Finds the zero based ordinal of a Member among its siblings.
     */
    public static int getMemberOrdinalInParent(SchemaReader reader,
                                               Member member) {
        Member parent = member.getParentMember();
        Member[] siblings =  (parent == null)
            ? reader.getHierarchyRootMembers(member.getHierarchy())
            : reader.getMemberChildren(parent);

        for (int i = 0; i < siblings.length; i++) {
            if (siblings[i] == member) {
                return i;
            }
        }
        throw Util.newInternal(
                "could not find member " + member + " amongst its siblings");
    }

    /**
     * returns the first descendant on the level underneath parent.
     * If parent = [Time].[1997] and level = [Time].[Month], then
     * the member [Time].[1997].[Q1].[1] will be returned
     */
    public static Member getFirstDescendantOnLevel(SchemaReader reader,
                                                   Member parent,
                                                   Level level) {
        Member m = parent;
        while (m.getLevel() != level) {
            Member[] children = reader.getMemberChildren(m);
            m = children[0];
        }
        return m;
    }

    /**
     * Returns whether a string is null or empty.
     */
    public static boolean isEmpty(String s) {
        return (s == null) || (s.length() == 0);
    }

    /**
     * Encloses a value in single-quotes, to make a SQL string value. Examples:
     * <code>singleQuoteForSql(null)</code> yields <code>NULL</code>;
     * <code>singleQuoteForSql("don't")</code> yields <code>'don''t'</code>.
     */
    public static String singleQuoteString(String val) {
        StringBuffer buf = new StringBuffer(64);
        singleQuoteString(val, buf);
        return buf.toString();
    }

    /**
     * Encloses a value in single-quotes, to make a SQL string value. Examples:
     * <code>singleQuoteForSql(null)</code> yields <code>NULL</code>;
     * <code>singleQuoteForSql("don't")</code> yields <code>'don''t'</code>.
     */
    public static void singleQuoteString(String val, StringBuffer buf) {
        buf.append('\'');

        String s0 = replace(val, "'", "''");
        buf.append(s0);

        buf.append('\'');
    }

    /**
     * Creates a random number generator.
     *
     * @param seed Seed for random number generator.
     *   If 0, generate a seed from the system clock and print the value
     *   chosen. (This is effectively non-deterministic.)
     *   If -1, generate a seed from an internal random number generator.
     *   (This is deterministic, but ensures that different tests have
     *   different seeds.)
     *
     * @return A random number generator.
     */
    public static Random createRandom(long seed) {
        if (seed == 0) {
            seed = System.nanoTime();
            System.out.println("random: seed=" + seed);
        } else if (seed == -1 && metaRandom != null) {
            seed = metaRandom.nextLong();
        }
        return new Random(seed);
    }

    /**
     * A <code>NullCellValue</code> is a placeholder value used when cells have
     * a null value. It is a singleton.
     */
    public static class NullCellValue implements Comparable {
        public String toString() {
            return "#NULL";
        }

        public int compareTo(Object o) {
            // Null is less than every other value.
            if (o == this) {
                return 0;
            } else {
                return -1;
            }
        }
    };

    public static class ErrorCellValue {
        public String toString() {
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
    public static String[] convertStackToString(Throwable e) {
        List list = new ArrayList();
        while (e != null) {
            String sMsg = getErrorMessage(e);
            list.add(sMsg);
            e = e.getCause();
        }
        return (String[]) list.toArray(new String[list.size()]);
    }

    /**
     * Constructs the message associated with an arbitrary Java error, making
     * up one based on the stack trace if there is none. As
     * {@link #getErrorMessage(Throwable,boolean)}, but does not print the
     * class name if the exception is derived from {@link java.sql.SQLException}
     * or is exactly a {@link java.lang.Exception}.
     **/
    public static String getErrorMessage(Throwable err) {
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
    public static String getErrorMessage(Throwable err,
                                         boolean prependClassName) {
        String errMsg = err.getMessage();
        if ((errMsg == null) || (err instanceof RuntimeException)) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            err.printStackTrace(pw);
            return sw.toString();
        } else {
            return (prependClassName)
                ? err.getClass().getName() + ": " + errMsg
                : errMsg;

        }
    }

    /**
     * Converts an expression to a string.
     */
    public static String unparse(Exp exp) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exp.unparse(pw);
        return sw.toString();
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
    public static class PropertyList {
        List list = new ArrayList();

        public String get(String key) {
            for (int i = 0, n = list.size(); i < n; i++) {
                String[] pair = (String[]) list.get(i);
                if (pair[0].equalsIgnoreCase(key)) {
                    return pair[1];
                }
            }
            return (key.equalsIgnoreCase("Provider"))
                ? "MSDASQL"
                : null;
        }

        public String put(String key, String value) {
            for (int i = 0, n = list.size(); i < n; i++) {
                String[] pair = (String[]) list.get(i);
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
            list.add(new String[] {key, value});
            return null;
        }

        public String toString() {
            StringBuffer sb = new StringBuffer(64);
            for (int i = 0, n = list.size(); i < n; i++) {
                String[] pair = (String[]) list.get(i);
                if (i > 0) {
                    sb.append("; ");
                }
                sb.append(pair[0]);
                sb.append('=');
                sb.append(pair[1]);
            }
            return sb.toString();
        }

        public Iterator iterator() {
            return list.iterator();
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
     * a look at the <code>mondrian.olap.UtilTestCase</code> test case.
     **/
    public static PropertyList parseConnectString(String s) {
        return new ConnectStringParser(s).parse();
    }

    private static class ConnectStringParser {
        private final String s;
        private final int n;
        private int i;
        private final StringBuffer nameBuf;
        private final StringBuffer valueBuf;

        private ConnectStringParser(String s) {
            this.s = s;
            this.i = 0;
            this.n = s.length();
            this.nameBuf = new StringBuffer(64);
            this.valueBuf = new StringBuffer(64);
        }

        PropertyList parse() {
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
                String value;
                int semi = s.indexOf(';', i);
                if (semi >= 0) {
                    value = s.substring(i, semi);
                    i = semi + 1;
                } else {
                    value = s.substring(i);
                    i = n;
                }
                return value.trim();
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
     * Converts a list, which may or may not be mutable, into a mutable list.
     * Non-mutable lists are returned by, for example,
     * {@link List#subList}, {@link Arrays#asList},
     * {@link Collections#unmodifiableList}.
     */
    public static List makeMutable(List list) {
        return (list instanceof ArrayList)
            ? list
            : new ArrayList(list);
    }

    /**
     * Combines two integers into a hash code.
     */
    public static int hash(int i, int j) {
        return (i << 4) ^ j;
    }

    /**
     * Computes a hash code from an existing hash code and an object (which
     * may be null).
     */
    public static int hash(int h, Object o) {
        int k = (o == null) ? 0 : o.hashCode();
        return ((h << 4) | h) ^ k;
    }

    /**
     * Computes a hash code from an existing hash code and an array of objects
     * (which may be null).
     */
    public static int hashArray(int h, Object [] a) {
        // The hashcode for a null array and an empty array should be different
        // than h, so use magic numbers.
        if (a == null) {
            return hash(h, 19690429);
        }
        if (a.length == 0) {
            return hash(h, 19690721);
        }
        for (int i = 0; i < a.length; i++) {
            h = hash(h, a[i]);
        }
        return h;
    }

    /**
     * Returns the cumulative amount of time spent accessing the database.
     */
    public static long dbTimeMillis()
    {
        return databaseMillis;
    }

    /**
     * Adds to the cumulative amount of time spent accessing the database.
     */
    public static void addDatabaseTime(long millis)
    {
        databaseMillis += millis;
    }

    /**
     * Returns the system time less the time spent accessing the database.
     * Use this method to figure out how long an operation took: call this
     * method before an operation and after an operation, and the difference
     * is the amount of non-database time spent.
     */
    public static long nonDbTimeMillis()
    {
        final long systemMillis = System.currentTimeMillis();
        return systemMillis - databaseMillis;
    }

    /**
     * Creates a very simple implementation of {@link Validator}. (Only
     * useful for resolving trivial expressions.)
     */
    public static Validator createSimpleValidator(final FunTable funTable) {
        return new Validator() {
            public Query getQuery() {
                throw new UnsupportedOperationException();
            }

            public Exp validate(Exp exp, boolean scalar) {
                return exp;
            }

            public Parameter validate(Parameter parameter) {
                return parameter;
            }

            public void validate(MemberProperty memberProperty) {
            }

            public void validate(QueryAxis axis) {
            }

            public void validate(Formula formula) {
            }

            public Exp convert(Exp fromExp, int to) {
                return FunUtil.convert(fromExp, to, this);
            }

            public boolean canConvert(Exp fromExp, int to, int[] conversionCount) {
                return true;
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
}

// End Util.java
