/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

import org.apache.commons.vfs.*;
import org.apache.commons.vfs.provider.http.HttpFileObject;
import org.apache.log4j.Logger;
import org.eigenbase.xom.XOMUtil;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.math.BigDecimal;
import java.lang.reflect.*;

import mondrian.olap.fun.*;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.spi.UserDefinedFunction;
import mondrian.mdx.*;
import mondrian.util.*;

/**
 * Utility functions used throughout mondrian. All methods are static.
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 */
public class Util extends XOMUtil {

    public static final String nl = System.getProperty("line.separator");

    private static final Logger LOGGER = Logger.getLogger(Util.class);

    /**
     * Placeholder which indicates a value NULL.
     */
    public static final Object nullValue = new Double(FunUtil.DoubleNull);

    /**
     * Placeholder which indicates an EMPTY value.
     */
    public static final Object EmptyValue = new Double(FunUtil.DoubleEmpty);

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

    /**
     * Whether we are running a version of Java before 1.5.
     * <p/>
     * <p>If this variable is true, we will be running retroweaver. Retroweaver
     * has some problems involving {@link java.util.EnumSet}.
     */
    public static final boolean PreJdk15 =
        System.getProperty("java.version").startsWith("1.4");

    /**
     * What version of JDBC? Returns 4 in JDK 1.6 and higher, 3 otherwise.
     */
    public static final int JdbcVersion =
        System.getProperty("java.version").compareTo("1.6") >= 0
            ? 4
            : 3;

    /**
     * Whether the code base has re-engineered using retroweaver.
     * If this is the case, some functionality is not available.
     */
    public static final boolean Retrowoven =
        Access.class.getSuperclass().getName().equals(
            "net.sourceforge.retroweaver.runtime.java.lang.Enum");

    private static final UtilCompatible compatible;

    static {
        String className;
        if (PreJdk15 || Retrowoven) {
            className = "mondrian.util.UtilCompatibleJdk14";
        } else {
            className = "mondrian.util.UtilCompatibleJdk15";
        }
        try {
            Class<UtilCompatible> clazz =
                (Class<UtilCompatible>) Class.forName(className);
            compatible = clazz.newInstance();
        } catch (ClassNotFoundException e) {
            throw Util.newInternal(e, "Could not load '" + className + "'");
        } catch (InstantiationException e) {
            throw Util.newInternal(e, "Could not load '" + className + "'");
        } catch (IllegalAccessException e) {
            throw Util.newInternal(e, "Could not load '" + className + "'");
        }
    }

    public static boolean isNull(Object o) {
        return o == null || o == nullValue;
    }

    /**
     * Returns whether a list is strictly sorted.
     *
     * @param list List
     * @return whether list is sorted
     */
    public static <T> boolean isSorted(List<T> list) {
        T prev = null;
        for (T t : list) {
            if (prev != null
                && ((Comparable<T>) prev).compareTo(t) >= 0)
            {
                return false;
            }
            prev = t;
        }
        return true;
    }

    /**
     * Encodes string for MDX (escapes ] as ]] inside a name).
     */
    public static String mdxEncodeString(String st) {
        StringBuilder retString = new StringBuilder(st.length() + 20);
        for (int i = 0; i < st.length(); i++) {
            char c = st.charAt(i);
            if ((c == ']')
                && ((i + 1) < st.length())
                && (st.charAt(i + 1) != '.'))
            {
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
        StringBuilder buf = new StringBuilder(val.length() + 20);
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
        StringBuilder buf = new StringBuilder(id.length() + 20);
        quoteMdxIdentifier(id, buf);
        return buf.toString();
    }

    public static void quoteMdxIdentifier(String id, StringBuilder buf) {
        buf.append('[');
        int start = buf.length();
        buf.append(id);
        replace(buf, start, "]", "]]");
        buf.append(']');
    }

    /**
     * Return identifiers quoted in [...].[...].  For example, {"Store", "USA",
     * "California"} becomes "[Store].[USA].[California]".
     */
    public static String quoteMdxIdentifier(List<Id.Segment> ids) {
        StringBuilder sb = new StringBuilder(64);
        quoteMdxIdentifier(ids, sb);
        return sb.toString();
    }

    public static void quoteMdxIdentifier(
        List<Id.Segment> ids,
        StringBuilder sb)
    {
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(ids.get(i).toString());
        }
    }

    /**
     * Returns true if two objects are equal, or are both null.
     */
    public static boolean equals(Object s, Object t) {
        return (s == null) ? (t == null) : s.equals(t);
    }

    /**
     * Returns whether two arrays have equal contents. Elements may be null.
     *
     * @param a1 First array
     * @param a2 Second array
     * @param <T> Element type
     * @return Whether arrays are equal
     */
    public static <T> boolean equalArray(
        T[] a1,
        T[] a2)
    {
        if (a1.length != a2.length) {
            return false;
        }
        for (int i = 0; i < a1.length; ++i) {
            if (!equals(a1[i], a2[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if two strings are equal, or are both null.
     *
     * <p>The result is not affected by
     * {@link MondrianProperties#CaseSensitive the case sensitive option}; if
     * you wish to compare names, use {@link #equalName(String, String)}.
     */
    public static boolean equals(String s, String t) {
        return equals((Object) s, (Object) t);
    }

    /**
     * Returns whether two names are equal.
     * Takes into account the
     * {@link MondrianProperties#CaseSensitive case sensitive option}.
     * Names may be null.
     */
    public static boolean equalName(String s, String t) {
        if (s == null) {
            return t == null;
        }
        boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();
        return caseSensitive ? s.equals(t) : s.equalsIgnoreCase(t);
    }

    /**
     * Tests two strings for equality, optionally ignoring case.
     *
     * @param s First string
     * @param t Second string
     * @param matchCase Whether to perform case-sensitive match
     * @return Whether strings are equal
     */
    public static boolean equal(String s, String t, boolean matchCase) {
        return matchCase ? s.equals(t) : s.equalsIgnoreCase(t);
    }

    /**
     * Compares two names.  if case sensitive flag is false,
     * apply finer grain difference with case sensitive
     * Takes into account the {@link MondrianProperties#CaseSensitive case
     * sensitive option}.
     * Names must not be null.
     */
    public static int caseSensitiveCompareName(String s, String t) {
        boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();
        if (caseSensitive) {
            return s.compareTo(t);
        } else {
            int v = s.compareToIgnoreCase(t);
            // if ignore case returns 0 compare in a case sensitive manner
            // this was introduced to solve an issue with Member.equals()
            // and Member.compareTo() not agreeing with each other
            return v == 0 ? s.compareTo(t) : v;
        }
    }

    /**
     * Compares two names.
     * Takes into account the {@link MondrianProperties#CaseSensitive case
     * sensitive option}.
     * Names must not be null.
     */
    public static int compareName(String s, String t) {
        boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();
        return caseSensitive ? s.compareTo(t) : s.compareToIgnoreCase(t);
    }

    /**
     * Generates a normalized form of a name, for use as a key into a map.
     * Returns the upper case name if
     * {@link MondrianProperties#CaseSensitive} is true, the name unchanged
     * otherwise.
     */
    public static String normalizeName(String s) {
        return MondrianProperties.instance().CaseSensitive.get()
            ? s
            : s.toUpperCase();
    }

    /**
     * Returns the result of ((Comparable) k1).compareTo(k2), with
     * special-casing for the fact that Boolean only became
     * comparable in JDK 1.5.
     *
     * @see Comparable#compareTo
     */
    public static int compareKey(Object k1, Object k2) {
        if (k1 instanceof Boolean) {
            // Luckily, "F" comes before "T" in the alphabet.
            k1 = k1.toString();
            k2 = k2.toString();
        }
        return ((Comparable) k1).compareTo(k2);
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
        StringBuilder sb = new StringBuilder(s.length() + 20);
        int start = 0;
        char[] chars = s.toCharArray();
        final int step = find.length();
        if (step == 0) {
            // Special case where find is "".
            sb.append(s);
            replace(sb, 0, find, replace);
        } else {
            for (;;) {
                sb.append(chars, start, found - start);
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
     * @param start Ordinal within <code>find</code> to start searching
     * @param find String to find
     * @param replace String to replace it with
     * @return The string buffer
     */
    public static StringBuilder replace(
        StringBuilder buf,
        int start,
        String find,
        String replace)
    {
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

    public static List<Id.Segment> parseIdentifier(String s)  {
        if (!s.startsWith("[")) {
            return Collections.singletonList(
                new Id.Segment(s, Id.Quoting.UNQUOTED));
        }

        List<Id.Segment> list = new ArrayList<Id.Segment>();
        int i = 0;
        Id.Quoting type;
        while (i < s.length()) {
            if (s.charAt(i) != '&' && s.charAt(i) != '[') {
                throw MondrianResource.instance().MdxInvalidMember.ex(s);
            }

            if (s.charAt(i) ==  '&') {
                i++;
                type = Id.Quoting.KEY;
            } else {
                type = Id.Quoting.QUOTED;
            }

            if (s.charAt(i) != '[') {
                throw MondrianResource.instance().MdxInvalidMember.ex(s);
            }

            int j = getEndIndex(s, i + 1);
            if (j == -1) {
                throw MondrianResource.instance().MdxInvalidMember.ex(s);
            }

            list.add(
                new Id.Segment(
                    replace(s.substring(i + 1, j), "]]", "]"),
                    type));

            i = j + 2;
        }
        return list;
    }

    private static int getEndIndex(String s, int i) {
        while (i < s.length()) {
            char ch = s.charAt(i);
            if (ch == ']') {
                if (i + 1 < s.length() && s.charAt(i + 1) == ']') {
                    // found ]] => skip
                    i += 2;
                } else {
                    return i;
                }
            } else {
                i++;
            }
        }
        return -1;
    }

    /**
     * Converts an array of name parts {"part1", "part2"} into a single string
     * "[part1].[part2]". If the names contain "]" they are escaped as "]]".
     */
    public static String implode(List<Id.Segment> names) {
        StringBuilder sb = new StringBuilder(64);
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) {
                sb.append(".");
            }
            // FIXME: should be:
            //   names.get(i).toString(sb);
            // but that causes some tests to fail
            quoteMdxIdentifier(names.get(i).name, sb);
        }
        return sb.toString();
    }

    public static String makeFqName(String name) {
        return quoteMdxIdentifier(name);
    }

    public static String makeFqName(OlapElement parent, String name) {
        if (parent == null) {
            return Util.quoteMdxIdentifier(name);
        } else {
            StringBuilder buf = new StringBuilder(64);
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
            StringBuilder buf = new StringBuilder(64);
            buf.append(parentUniqueName);
            buf.append('.');
            Util.quoteMdxIdentifier(name, buf);
            return buf.toString();
        }
    }

    public static OlapElement lookupCompound(
        SchemaReader schemaReader,
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category)
    {
        return lookupCompound(
            schemaReader, parent, names, failIfNotFound, category,
            MatchType.EXACT);
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
     *
     * @pre parent != null
     * @post !(failIfNotFound && return == null)
     *
     * @see #parseIdentifier(String)
     */
    public static OlapElement lookupCompound(
        SchemaReader schemaReader,
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category,
        MatchType matchType)
    {
        Util.assertPrecondition(parent != null, "parent != null");

        if (LOGGER.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(64);
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
        for (int i = 0; i < names.size(); i++) {
            Id.Segment name = names.get(i);
            OlapElement child =
                schemaReader.getElementChild(parent, name, matchType);
            // if we're doing a non-exact match, and we find a non-exact
            // match, then for an after match, return the first child
            // of each subsequent level; for a before match, return the
            // last child
            if (child instanceof Member
                && !matchType.isExact()
                && !Util.equalName(child.getName(), name.name))
            {
                Member bestChild = (Member) child;
                for (int j = i + 1; j < names.size(); j++) {
                    List<Member> childrenList =
                        schemaReader.getMemberChildren(bestChild);
                    FunUtil.hierarchizeMemberList(childrenList, false);
                    if (matchType == MatchType.AFTER) {
                        bestChild = childrenList.get(0);
                    } else {
                        bestChild =
                            childrenList.get(childrenList.size() - 1);
                    }
                    if (bestChild == null) {
                        child = null;
                        break;
                    }
                }
                parent = bestChild;
                break;
            }
            if (child == null) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "Util.lookupCompound: "
                        + "parent.name="
                        + parent.getName()
                        + " has no child with name="
                        + name);
                }

                if (!failIfNotFound) {
                    return null;
                } else if (category == Category.Member) {
                    throw MondrianResource.instance().MemberNotFound.ex(
                        quoteMdxIdentifier(names));
                } else {
                    throw MondrianResource.instance().MdxChildObjectNotFound
                        .ex(name.name, parent.getQualifiedName());
                }
            }
            parent = child;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "Util.lookupCompound: "
                + "found child.name="
                + parent.getName()
                + ", child.class="
                + parent.getClass().getName());
        }

        switch (category) {
        case Category.Dimension:
            if (parent instanceof Dimension) {
                return parent;
            } else if (parent instanceof Hierarchy) {
                return parent.getDimension();
            } else if (failIfNotFound) {
                throw Util.newError(
                    "Can not find dimension '" + implode(names) + "'");
            } else {
                return null;
            }
        case Category.Hierarchy:
            if (parent instanceof Hierarchy) {
                return parent;
            } else if (parent instanceof Dimension) {
                return parent.getHierarchy();
            } else if (failIfNotFound) {
                throw Util.newError(
                    "Can not find hierarchy '" + implode(names) + "'");
            } else {
                return null;
            }
        case Category.Level:
            if (parent instanceof Level) {
                return parent;
            } else if (failIfNotFound) {
                throw Util.newError(
                    "Can not find level '" + implode(names) + "'");
            } else {
                return null;
            }
        case Category.Member:
            if (parent instanceof Member) {
                return parent;
            } else if (failIfNotFound) {
                throw MondrianResource.instance().MdxCantFindMember.ex(
                    implode(names));
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

    public static OlapElement lookup(Query q, List<Id.Segment> nameParts) {
        final Exp exp = lookup(q, nameParts, false);
        if (exp instanceof MemberExpr) {
            MemberExpr memberExpr = (MemberExpr) exp;
            return memberExpr.getMember();
        } else if (exp instanceof LevelExpr) {
            LevelExpr levelExpr = (LevelExpr) exp;
            return levelExpr.getLevel();
        } else if (exp instanceof HierarchyExpr) {
            HierarchyExpr hierarchyExpr = (HierarchyExpr) exp;
            return hierarchyExpr.getHierarchy();
        } else if (exp instanceof DimensionExpr) {
            DimensionExpr dimensionExpr = (DimensionExpr) exp;
            return dimensionExpr.getDimension();
        } else {
            throw Util.newInternal("Not an olap element: " + exp);
        }
    }

    /**
     * Converts an identifier into an expression by resolving its parts into
     * an OLAP object (dimension, hierarchy, level or member) within the
     * context of a query.
     *
     * <p>If <code>allowProp</code> is true, also allows property references
     * from valid members, for example
     * <code>[Measures].[Unit Sales].FORMATTED_VALUE</code>.
     * In this case, the result will be a {@link ResolvedFunCall}.
     *
     * @param q Query expression belongs to
     * @param nameParts Parts of the identifier
     * @param allowProp Whether to allow property references
     * @return OLAP object or property reference
     */
    public static Exp lookup(
        Query q,
        List<Id.Segment> nameParts,
        boolean allowProp)
    {
        // First, look for a calculated member defined in the query.
        final String fullName = quoteMdxIdentifier(nameParts);
        // Look for any kind of object (member, level, hierarchy,
        // dimension) in the cube. Use a schema reader without restrictions.
        final SchemaReader schemaReader = q.getSchemaReader(false);
        OlapElement olapElement =
            schemaReader.lookupCompound(
                q.getCube(), nameParts, false, Category.Unknown);
        if (olapElement != null) {
            final SchemaReader accessConSchemaReader = q.getSchemaReader(true);
            Role role = accessConSchemaReader.getRole();
            if (!role.canAccess(olapElement)) {
                olapElement = null;
            }
            if (olapElement instanceof Member) {
                olapElement =
                    accessConSchemaReader.substitute((Member) olapElement);
            }
        }
        if (olapElement == null) {
            if (allowProp && nameParts.size() > 1) {
                List<Id.Segment> namePartsButOne =
                    nameParts.subList(0, nameParts.size() - 1);
                final String propertyName =
                    nameParts.get(nameParts.size() - 1).name;
                olapElement =
                    schemaReader.lookupCompound(
                        q.getCube(), namePartsButOne, false, Category.Member);
                if (olapElement != null
                    && isValidProperty((Member) olapElement, propertyName))
                {
                    return new UnresolvedFunCall(
                        propertyName, Syntax.Property, new Exp[] {
                            createExpr(olapElement)});
                }
            }
            // if we're in the middle of loading the schema, the property has
            // been set to ignore invalid members, and the member is
            // non-existent, return the null member corresponding to the
            // hierarchy of the element we're looking for; locate the
            // hierarchy by incrementally truncating the name of the element
            if (q.ignoreInvalidMembers()) {
                int nameLen = nameParts.size() - 1;
                olapElement = null;
                while (nameLen > 0 && olapElement == null) {
                    List<Id.Segment> partialName =
                        nameParts.subList(0, nameLen);
                    olapElement = schemaReader.lookupCompound(
                        q.getCube(), partialName, false, Category.Unknown);
                    nameLen--;
                }
                if (olapElement != null) {
                    olapElement = olapElement.getHierarchy().getNullMember();
                } else {
                    throw MondrianResource.instance().MdxChildObjectNotFound
                        .ex(fullName, q.getCube().getQualifiedName());
                }
            } else {
                throw MondrianResource.instance().MdxChildObjectNotFound.ex(
                    fullName, q.getCube().getQualifiedName());
            }
        }
        // keep track of any measure members referenced; these will be used
        // later to determine if cross joins on virtual cubes can be
        // processed natively
        q.addMeasuresMembers(olapElement);
        return createExpr(olapElement);
    }

    /**
     * Looks up a cube in a schema reader.
     *
     * @param cubeName Cube name
     * @param fail Whether to fail if not found.
     * @return Cube, or null if not found
     */
    static Cube lookupCube(
        SchemaReader schemaReader,
        String cubeName,
        boolean fail)
    {
        for (Cube cube : schemaReader.getCubes()) {
            if (Util.compareName(cube.getName(), cubeName) == 0) {
                return cube;
            }
        }
        if (fail) {
            throw MondrianResource.instance().MdxCubeNotFound.ex(cubeName);
        }
        return null;
    }

    /**
     * Converts an olap element (dimension, hierarchy, level or member) into
     * an expression representing a usage of that element in an MDX statement.
     */
    public static Exp createExpr(OlapElement element)
    {
        if (element instanceof Member) {
            Member member = (Member) element;
            return new MemberExpr(member);
        } else if (element instanceof Level) {
            Level level = (Level) element;
            return new LevelExpr(level);
        } else if (element instanceof Hierarchy) {
            Hierarchy hierarchy = (Hierarchy) element;
            return new HierarchyExpr(hierarchy);
        } else if (element instanceof Dimension) {
            Dimension dimension = (Dimension) element;
            return new DimensionExpr(dimension);
        } else if (element instanceof NamedSet) {
            NamedSet namedSet = (NamedSet) element;
            return new NamedSetExpr(namedSet);
        } else {
            throw Util.newInternal("Unexpected element type: " + element);
        }
    }

    public static Member lookupHierarchyRootMember(
        SchemaReader reader, Hierarchy hierarchy, Id.Segment memberName)
    {
        return lookupHierarchyRootMember(
            reader, hierarchy, memberName, MatchType.EXACT);
    }

    /**
     * Finds a root member of a hierarchy with a given name.
     *
     * @param hierarchy Hierarchy
     * @param memberName Name of root member
     * @return Member, or null if not found
     */
    public static Member lookupHierarchyRootMember(
        SchemaReader reader,
        Hierarchy hierarchy,
        Id.Segment memberName,
        MatchType matchType)
    {
        // Lookup member at first level.
        //
        // Don't use access control. Suppose we cannot see the 'nation' level,
        // we still want to be able to resolve '[Customer].[USA].[CA]'.
        List<Member> rootMembers = reader.getHierarchyRootMembers(hierarchy);

        // if doing an inexact search on a non-all hieararchy, create
        // a member corresponding to the name we're searching for so
        // we can use it in a hierarchical search
        Member searchMember = null;
        if (!matchType.isExact()
            && !hierarchy.hasAll()
            && !rootMembers.isEmpty())
        {
            searchMember =
                hierarchy.createMember(
                    null,
                    rootMembers.get(0).getLevel(),
                    memberName.name,
                    null);
        }

        int bestMatch = -1;
        int k = -1;
        for (Member rootMember : rootMembers) {
            ++k;
            int rc;
            // when searching on the ALL hierarchy, match must be exact
            if (matchType.isExact() || hierarchy.hasAll()) {
                rc = rootMember.getName().compareToIgnoreCase(memberName.name);
            } else {
                rc = FunUtil.compareSiblingMembers(
                    rootMember,
                    searchMember);
            }
            if (rc == 0) {
                return rootMember;
            }
            if (!hierarchy.hasAll()) {
                if (matchType == MatchType.BEFORE) {
                    if (rc < 0
                        && (bestMatch == -1
                            || FunUtil.compareSiblingMembers(
                                rootMember,
                                rootMembers.get(bestMatch)) > 0))
                    {
                        bestMatch = k;
                    }
                } else if (matchType == MatchType.AFTER) {
                    if (rc > 0
                        && (bestMatch == -1
                            || FunUtil.compareSiblingMembers(
                                rootMember,
                                rootMembers.get(bestMatch)) < 0))
                    {
                        bestMatch = k;
                    }
                }
            }
        }

        if (matchType == MatchType.EXACT_SCHEMA) {
            return null;
        }

        if (matchType != MatchType.EXACT && bestMatch != -1) {
            return rootMembers.get(bestMatch);
        }
        // If the first level is 'all', lookup member at second level. For
        // example, they could say '[USA]' instead of '[(All
        // Customers)].[USA]'.
        return (rootMembers.size() == 1 && rootMembers.get(0).isAll())
            ? reader.lookupMemberChildByName(
                rootMembers.get(0),
                memberName,
                matchType)
            : null;
    }

    /**
     * Finds a named level in this hierarchy. Returns null if there is no
     * such level.
     */
    public static Level lookupHierarchyLevel(Hierarchy hierarchy, String s) {
        final Level[] levels = hierarchy.getLevels();
        for (Level level : levels) {
            if (level.getName().equalsIgnoreCase(s)) {
                return level;
            }
        }
        return null;
    }



    /**
     * Finds the zero based ordinal of a Member among its siblings.
     */
    public static int getMemberOrdinalInParent(
        SchemaReader reader,
        Member member)
    {
        Member parent = member.getParentMember();
        List<Member> siblings =
            (parent == null)
            ? reader.getHierarchyRootMembers(member.getHierarchy())
            : reader.getMemberChildren(parent);

        for (int i = 0; i < siblings.size(); i++) {
            if (siblings.get(i).equals(member)) {
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
    public static Member getFirstDescendantOnLevel(
        SchemaReader reader,
        Member parent,
        Level level)
    {
        Member m = parent;
        while (m.getLevel() != level) {
            List<Member> children = reader.getMemberChildren(m);
            m = children.get(0);
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
        StringBuilder buf = new StringBuilder(64);
        singleQuoteString(val, buf);
        return buf.toString();
    }

    /**
     * Encloses a value in single-quotes, to make a SQL string value. Examples:
     * <code>singleQuoteForSql(null)</code> yields <code>NULL</code>;
     * <code>singleQuoteForSql("don't")</code> yields <code>'don''t'</code>.
     */
    public static void singleQuoteString(String val, StringBuilder buf) {
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
            seed = new Random().nextLong();
            System.out.println("random: seed=" + seed);
        } else if (seed == -1 && metaRandom != null) {
            seed = metaRandom.nextLong();
        }
        return new Random(seed);
    }

    /**
     * Returns whether a property is valid for a given member.
     * It is valid if the property is defined at the member's level or at
     * an ancestor level, or if the property is a standard property such as
     * "FORMATTED_VALUE".
     *
     * @param member Member
     * @param propertyName Property name
     * @return Whether property is valid
     */
    public static boolean isValidProperty(
        Member member,
        String propertyName)
    {
        return lookupProperty(member.getLevel(), propertyName) != null;
    }

    /**
     * Finds a member property called <code>propertyName</code> at, or above,
     * <code>level</code>.
     */
    protected static Property lookupProperty(
        Level level,
        String propertyName)
    {
        do {
            Property[] properties = level.getProperties();
            for (Property property : properties) {
                if (property.getName().equals(propertyName)) {
                    return property;
                }
            }
            level = level.getParentLevel();
        } while (level != null);
        // Now try a standard property.
        boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();
        final Property property = Property.lookup(propertyName, caseSensitive);
        if (property != null
            && property.isMemberProperty()
            && property.isStandard())
        {
            return property;
        }
        return null;
    }

    /**
     * Insert a call to this method if you want to flag a piece of
     * undesirable code.
     *
     * @deprecated
     */
    public static void deprecated(String reason) {
        throw new UnsupportedOperationException(reason);
    }

    public static List<Member> addLevelCalculatedMembers(
        SchemaReader reader,
        Level level,
        List<Member> members)
    {
        List<Member> calcMembers =
            reader.getCalculatedMembers(level.getHierarchy());
        List<Member> calcMembersInThisLevel = new ArrayList<Member>();
        for (Member calcMember : calcMembers) {
            if (calcMember.getLevel().equals(level)) {
                calcMembersInThisLevel.add(calcMember);
            }
        }
        if (!calcMembersInThisLevel.isEmpty()) {
            List<Member> newMemberList =
                new ConcatenableList<Member>();
            newMemberList.addAll(members);
            newMemberList.addAll(calcMembersInThisLevel);
            return newMemberList;
        }
        return members;
    }

    /**
     * Returns an exception which indicates that a particular piece of
     * functionality should work, but a developer has not implemented it yet.
     */
    public static RuntimeException needToImplement(Object o) {
        throw new UnsupportedOperationException("need to implement " + o);
    }

    /**
     * Returns an exception indicating that we didn't expect to find this value
     * here.
     */
    public static <T extends Enum<T>> RuntimeException badValue(
        Enum<T> anEnum)
    {
        return Util.newInternal(
            "Was not expecting value '" + anEnum
            + "' for enumeration '" + anEnum.getDeclaringClass().getName()
            + "' in this context");
    }

    /**
     * Masks Mondrian's version number from a string.
     *
     * @param str String
     * @return String with each occurrence of mondrian's version number
     *    (e.g. "2.3.0.0") replaced with "${mondrianVersion}"
     */
    public static String maskVersion(String str) {
        MondrianServer.MondrianVersion mondrianVersion =
            MondrianServer.forConnection(null).getVersion();
        String versionString = mondrianVersion.getVersionString();
        return replace(str, versionString, "${mondrianVersion}");
    }

    /**
     * Converts a list of SQL-style patterns into a Java regular expression.
     *
     * <p>For example, {"Foo_", "Bar%BAZ"} becomes "Foo.|Bar.*BAZ".
     *
     * @param wildcards List of SQL-style wildcard expressions
     * @return Regular expression
     */
    public static String wildcardToRegexp(List<String> wildcards) {
        StringBuilder buf = new StringBuilder();
        for (String value : wildcards) {
            if (buf.length() > 0) {
                buf.append('|');
            }
            int i = 0;
            while (true) {
                int percent = value.indexOf('%', i);
                int underscore = value.indexOf('_', i);
                if (percent == -1 && underscore == -1) {
                    if (i < value.length()) {
                        buf.append(quotePattern(value.substring(i)));
                    }
                    break;
                }
                if (underscore >= 0 && (underscore < percent || percent < 0)) {
                    if (i < underscore) {
                        buf.append(
                            quotePattern(value.substring(i, underscore)));
                    }
                    buf.append('.');
                    i = underscore + 1;
                } else if (percent >= 0
                    && (percent < underscore || underscore < 0))
                {
                    if (i < percent) {
                    buf.append(
                        quotePattern(value.substring(i, percent)));
                    }
                    buf.append(".*");
                    i = percent + 1;
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }
        return buf.toString();
    }

    /**
     * Converts a camel-case name to an upper-case name with underscores.
     *
     * <p>For example, <code>camelToUpper("FooBar")</code> returns "FOO_BAR".
     *
     * @param s Camel-case string
     * @return  Upper-case string
     */
    public static String camelToUpper(String s) {
        StringBuilder buf = new StringBuilder(s.length() + 10);
        int prevUpper = -1;
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > prevUpper + 1) {
                    buf.append('_');
                }
                prevUpper = i;
            } else {
                c = Character.toUpperCase(c);
            }
            buf.append(c);
        }
        return buf.toString();
    }

    /**
     * Parses a comma-separated list.
     *
     * <p>If a value contains a comma, escape it with a second comma. For
     * example, <code>parseCommaList("x,y,,z")</code> returns
     * <code>{"x", "y,z"}</code>.
     *
     * @param nameCommaList List of names separated by commas
     * @return List of names
     */
    public static List<String> parseCommaList(String nameCommaList) {
        if (nameCommaList.equals("")) {
            return Collections.emptyList();
        }
        if (nameCommaList.endsWith(",")) {
            // Special treatment for list ending in ",", because split ignores
            // entries after separator.
            final String zzz = "zzz";
            final List<String> list = parseCommaList(nameCommaList + zzz);
            String last = list.get(list.size() - 1);
            if (last.equals(zzz)) {
                list.remove(list.size() - 1);
            } else {
                list.set(
                    list.size() - 1,
                    last.substring(0, last.length() - zzz.length()));
            }
            return list;
        }
        List<String> names = new ArrayList<String>();
        final String[] strings = nameCommaList.split(",");
        for (String string : strings) {
            final int count = names.size();
            if (count > 0
                && names.get(count - 1).equals(""))
            {
                if (count == 1) {
                    if (string.equals("")) {
                        names.add("");
                    } else {
                        names.set(
                            0,
                            "," + string);
                    }
                } else {
                    names.set(
                        count - 2,
                        names.get(count - 2) + "," + string);
                    names.remove(count - 1);
                }
            } else {
                names.add(string);
            }
        }
        return names;
    }

    /**
     * Returns an annotation of a particular class on a method. Returns the
     * default value if the annotation is not present, or in JDK 1.4.
     *
     * @param method Method containing annotation
     * @param annotationClassName Name of annotation class to find
     * @param defaultValue Value to return if annotation is not present
     * @return value of annotation
     */
    public static <T> T getAnnotation(
        Method method,
        String annotationClassName,
        T defaultValue)
    {
        return compatible.getAnnotation(
            method, annotationClassName, defaultValue);
    }

    /**
     * Converts a list of a string.
     *
     * For example,
     * <code>commaList("foo", Arrays.asList({"a", "b"}))</code>
     * returns "foo(a, b)".
     *
     * @param s Prefix
     * @param list List
     * @return String representation of string
     */
    public static <T> String commaList(
        String s,
        List<T> list)
    {
        final StringBuilder buf = new StringBuilder(s);
        buf.append("(");
        int k = -1;
        for (T t : list) {
            if (++k > 0) {
                buf.append(", ");
            }
            buf.append(t);
        }
        buf.append(")");
        return buf.toString();
    }

    /**
     * Makes a name distinct from other names which have already been used
     * and shorter than a length limit, adds it to the list, and returns it.
     *
     * @param name Suggested name, may not be unique
     * @param maxLength Maximum length of generated name
     * @param nameList Collection of names already used
     *
     * @return Unique name
     */
    public static String uniquify(
        String name,
        int maxLength,
        Collection<String> nameList)
    {
        assert name != null;
        if (name.length() > maxLength) {
            name = name.substring(0, maxLength);
        }
        if (nameList.contains(name)) {
            String aliasBase = name;
            int j = 0;
            while (true) {
                name = aliasBase + j;
                if (name.length() > maxLength) {
                    aliasBase = aliasBase.substring(0, aliasBase.length() - 1);
                    continue;
                }
                if (!nameList.contains(name)) {
                    break;
                }
                j++;
            }
        }
        nameList.add(name);
        return name;
    }

    /**
     * Returns whether a collection contains precisely one distinct element.
     * Returns false if the collection is empty, or if it contains elements
     * that are not the same as each other.
     *
     * @param collection Collection
     * @return boolean true if all values are same
     */
    public static <T> boolean areOccurencesEqual(
        Collection<T> collection)
    {
        Iterator<T> it = collection.iterator();
        if (!it.hasNext()) {
            // Collection is empty
            return false;
        }
        T first = it.next();
        while (it.hasNext()) {
            T t = it.next();
            if (!t.equals(first)) {
                return false;
            }
        }
        return true;
    }

    public static class ErrorCellValue {
        public String toString() {
            return "#ERR";
        }
    }

    /**
     * Throws an internal error if condition is not true. It would be called
     * <code>assert</code>, but that is a keyword as of JDK 1.4.
     */
    public static void assertTrue(boolean b) {
        if (!b) {
            throw newInternal("assert failed");
        }
    }

    /**
     * Throws an internal error with the given messagee if condition is not
     * true. It would be called <code>assert</code>, but that is a keyword as
     * of JDK 1.4.
     */
    public static void assertTrue(boolean b, String message) {
        if (!b) {
            throw newInternal("assert failed: " + message);
        }
    }

    /**
     * Creates an internal error with a given message.
     */
    public static RuntimeException newInternal(String message) {
        return MondrianResource.instance().Internal.ex(message);
    }

    /**
     * Creates an internal error with a given message and cause.
     */
    public static RuntimeException newInternal(Throwable e, String message) {
        return MondrianResource.instance().Internal.ex(message, e);
    }

    /**
     * Creates a non-internal error. Currently implemented in terms of
     * internal errors, but later we will create resourced messages.
     */
    public static RuntimeException newError(String message) {
        return newInternal(message);
    }

    /**
     * Creates a non-internal error. Currently implemented in terms of
     * internal errors, but later we will create resourced messages.
     */
    public static RuntimeException newError(Throwable e, String message) {
        return newInternal(e, message);
    }

    /**
     * Returns an exception indicating that we didn't expect to find this value
     * here.
     *
     * @param value Value
     */
    public static RuntimeException unexpected(Enum value) {
        return Util.newInternal(
            "Was not expecting value '" + value
            + "' for enumeration '" + value.getClass().getName()
            + "' in this context");
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

    /**
     * Converts an error into an array of strings, the most recent error first.
     *
     * @param e the error; may be null. Errors are chained according to their
     *    {@link Throwable#getCause cause}.
     */
    public static String[] convertStackToString(Throwable e) {
        List<String> list = new ArrayList<String>();
        while (e != null) {
            String sMsg = getErrorMessage(e);
            list.add(sMsg);
            e = e.getCause();
        }
        return list.toArray(new String[list.size()]);
    }

    /**
     * Constructs the message associated with an arbitrary Java error, making
     * up one based on the stack trace if there is none. As
     * {@link #getErrorMessage(Throwable,boolean)}, but does not print the
     * class name if the exception is derived from {@link java.sql.SQLException}
     * or is exactly a {@link java.lang.Exception}.
     */
    public static String getErrorMessage(Throwable err) {
        boolean prependClassName =
            !(err instanceof java.sql.SQLException
              || err.getClass() == java.lang.Exception.class);
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
        Throwable err,
        boolean prependClassName)
    {
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
     * Converts an query to a string.
     */
    public static String unparse(Query query) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new QueryPrintWriter(sw);
        query.unparse(pw);
        return sw.toString();
    }

    /**
     * Creates a file-protocol URL for the given file.
     */
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
     */
    public static class PropertyList
        implements Iterable<Pair<String, String>>
    {
        List<Pair<String, String>> list =
            new ArrayList<Pair<String, String>>();

        public String get(String key) {
            return get(key, null);
        }

        public String get(String key, String defaultValue) {
            for (int i = 0, n = list.size(); i < n; i++) {
                Pair<String, String> pair = list.get(i);
                if (pair.left.equalsIgnoreCase(key)) {
                    return pair.right;
                }
            }
            return defaultValue;
        }

        public String put(String key, String value) {
            for (int i = 0, n = list.size(); i < n; i++) {
                Pair<String, String> pair = list.get(i);
                if (pair.left.equalsIgnoreCase(key)) {
                    String old = pair.right;
                    if (key.equalsIgnoreCase("Provider")) {
                        // Unlike all other properties, later values of
                        // "Provider" do not supersede
                    } else {
                        pair.right = value;
                    }
                    return old;
                }
            }
            list.add(new Pair<String, String>(key, value));
            return null;
        }

        public boolean remove(String key) {
            boolean found = false;
            for (int i = 0; i < list.size(); i++) {
                Pair<String, String> pair = list.get(i);
                if (pair.getKey().equalsIgnoreCase(key)) {
                    list.remove(i);
                    found = true;
                    --i;
                }
            }
            return found;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder(64);
            for (int i = 0, n = list.size(); i < n; i++) {
                Pair<String, String> pair = list.get(i);
                if (i > 0) {
                    sb.append("; ");
                }
                sb.append(pair.left);
                sb.append('=');

                final String right = pair.right;
                if (right == null) {
                    sb.append("'null'");
                } else {
                    // Quote a property value if is has a semi colon in it
                    // 'xxx;yyy'. Escape any single-quotes by doubling them.
                    final int needsQuote = right.indexOf(';');
                    if (needsQuote >= 0) {
                        // REVIEW: This logic leaves off the leading/trailing
                        //   quote if the property value already has a
                        //   leading/trailing quote. Doesn't seem right to me.
                        if (right.charAt(0) != '\'') {
                            sb.append("'");
                        }
                        sb.append(replace(right, "'", "''"));
                        if (right.charAt(right.length() - 1) != '\'') {
                            sb.append("'");
                        }
                    } else {
                        sb.append(right);
                    }
                }
            }
            return sb.toString();
        }

        public Iterator<Pair<String, String>> iterator() {
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
     */
    public static PropertyList parseConnectString(String s) {
        return new ConnectStringParser(s).parse();
    }

    private static class ConnectStringParser {
        private final String s;
        private final int n;
        private int i;
        private final StringBuilder nameBuf;
        private final StringBuilder valueBuf;

        private ConnectStringParser(String s) {
            this.s = s;
            this.i = 0;
            this.n = s.length();
            this.nameBuf = new StringBuilder(64);
            this.valueBuf = new StringBuilder(64);
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
            if (name == null) {
                return;
            }
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
         * doubled. Returns null if there is no name to read.
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
                        if (i >= n) {
                            // there is no name, e.g. trailing spaces after
                            // semicolon, 'x=1; y=2; '
                            return null;
                        }
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
                        "quoted value ended too soon, at position " + i
                        + " in '" + s + "'");
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
                "Connect string '" + s
                + "' contains unterminated quoted value '" + valueBuf.toString()
                + "'");
        }
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
        for (Object anA : a) {
            h = hash(h, anA);
        }
        return h;
    }

    /**
     * Returns the cumulative amount of time spent accessing the database.
     */
    public static long dbTimeMillis() {
        return databaseMillis;
    }

    /**
     * Adds to the cumulative amount of time spent accessing the database.
     */
    public static void addDatabaseTime(long millis) {
        databaseMillis += millis;
    }

    /**
     * Returns the system time less the time spent accessing the database.
     * Use this method to figure out how long an operation took: call this
     * method before an operation and after an operation, and the difference
     * is the amount of non-database time spent.
     */
    public static long nonDbTimeMillis() {
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
                return null;
            }

            public Exp validate(Exp exp, boolean scalar) {
                return exp;
            }

            public void validate(ParameterExpr parameterExpr) {
            }

            public void validate(MemberProperty memberProperty) {
            }

            public void validate(QueryAxis axis) {
            }

            public void validate(Formula formula) {
            }

            public FunDef getDef(Exp[] args, String name, Syntax syntax) {
                // Very simple resolution. Assumes that there is precisely
                // one resolver (i.e. no overloading) and no argument
                // conversions are necessary.
                List<Resolver> resolvers = funTable.getResolvers(name, syntax);
                final Resolver resolver = resolvers.get(0);
                final List<Resolver.Conversion> conversionList =
                    new ArrayList<Resolver.Conversion>();
                final FunDef def =
                    resolver.resolve(args, this, conversionList);
                assert conversionList.isEmpty();
                return def;
            }

            public boolean alwaysResolveFunDef() {
                return false;
            }

            public boolean canConvert(
                Exp fromExp,
                int to,
                List<Resolver.Conversion> conversions)
            {
                return true;
            }

            public boolean requiresExpression() {
                return false;
            }

            public FunTable getFunTable() {
                return funTable;
            }

            public Parameter createOrLookupParam(
                boolean definition,
                String name,
                Type type,
                Exp defaultExp,
                String description)
            {
                return null;
            }
        };
    }

    /**
     * Reads a Reader until it returns EOF and return the contents as a String.
     *
     * @param rdr  Reader to Read.
     * @param bufferSize size of buffer to allocate for reading.
     * @return content of Reader as String or null if Reader was empty.
     * @throws IOException
     */
    public static String readFully(final Reader rdr, final int bufferSize)
        throws IOException
    {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException(
                "Buffer size must be greater than 0");
        }

        final char[] buffer = new char[bufferSize];
        final StringBuilder buf = new StringBuilder(bufferSize);

        int len = rdr.read(buffer);
        while (len != -1) {
            buf.append(buffer, 0, len);
            len = rdr.read(buffer);
        }

        final String s = buf.toString();
        return (s.length() == 0) ? null : s;
    }


    /**
     * Read URL and return String containing content.
     *
     * @param urlStr actually a catalog URL
     * @return String containing content of catalog.
     * @throws MalformedURLException
     * @throws IOException
     */
    public static String readURL(final String urlStr)
        throws IOException
    {
        return readURL(urlStr, null);
    }

    /**
     * Returns the contents of a URL, substituting tokens.
     *
     * <p>Replaces the tokens "${key}" if "key" occurs in the key-value map.
     *
     * @param urlStr  URL string
     * @param map Key/value map
     * @return Contents of URL with tokens substituted
     * @throws MalformedURLException
     * @throws IOException
     */
    public static String readURL(final String urlStr, Map<String, String> map)
        throws IOException
    {
        final URL url = new URL(urlStr);
        return readURL(url, map);
    }

    /**
     * Returns the contents of a URL.
     *
     * @param url URL
     * @return Contents of URL
     * @throws IOException
     */
    public static String readURL(final URL url) throws IOException {
        return readURL(url, null);
    }

    /**
     * Returns the contents of a URL, substituting tokens.
     *
     * <p>Replaces the tokens "${key}" if "key" occurs in the key-value map.
     *
     * @param url URL
     * @param map Key/value map
     * @return Contents of URL with tokens substituted
     * @throws IOException
     */
    public static String readURL(
        final URL url,
        Map<String, String> map)
        throws IOException
    {
        final Reader r =
            new BufferedReader(new InputStreamReader(url.openStream()));
        final int BUF_SIZE = 8096;
        try {
            String xmlCatalog = readFully(r, BUF_SIZE);
            if (map != null) {
                xmlCatalog = Util.replaceProperties(xmlCatalog, map);
            }
            return xmlCatalog;
        } finally {
            r.close();
        }
    }

    /**
     * Gets content via Apache VFS. File must exist and have content
     *
     * @param url String
     * @return Apache VFS FileContent for further processing
     * @throws FileSystemException
     */
    public static InputStream readVirtualFile(String url)
        throws FileSystemException
    {
        // Treat catalogUrl as an Apache VFS (Virtual File System) URL.
        // VFS handles all of the usual protocols (http:, file:)
        // and then some.
        FileSystemManager fsManager = VFS.getManager();
        if (fsManager == null) {
            throw newError("Cannot get virtual file system manager");
        }

        // Workaround VFS bug.
        if (url.startsWith("file://localhost")) {
            url = url.substring("file://localhost".length());
        }
        if (url.startsWith("file:")) {
            url = url.substring("file:".length());
        }

        //work around for VFS bug not closing http sockets
        // (Mondrian-585)
        if (url.startsWith("http")) {
            try {
                return new URL(url).openStream();
            } catch (IOException e) {
                throw newError(
                    "Could not read URL: " + url);
            }
        }

        File userDir = new File("").getAbsoluteFile();
        FileObject file = fsManager.resolveFile(userDir, url);
        FileContent fileContent = null;
        try {
            // Because of VFS caching, make sure we refresh to get the latest
            // file content. This refresh may possibly solve the following
            // workaround for defect MONDRIAN-508, but cannot be tested, so we
            // will leave the work around for now.
            file.refresh();

            // Workaround to defect MONDRIAN-508. For HttpFileObjects, verifies
            // the URL of the file retrieved matches the URL passed in.  A VFS
            // cache bug can cause it to treat URLs with different parameters
            // as the same file (e.g. http://blah.com?param=A,
            // http://blah.com?param=B)
            if (file instanceof HttpFileObject
                && !file.getName().getURI().equals(url))
            {
                fsManager.getFilesCache().removeFile(
                    file.getFileSystem(),  file.getName());

                file = fsManager.resolveFile(userDir, url);
            }

            if (!file.isReadable()) {
                throw newError(
                    "Virtual file is not readable: " + url);
            }

            fileContent = file.getContent();
        } finally {
            file.close();
        }

        if (fileContent == null) {
            throw newError(
                "Cannot get virtual file content: " + url);
        }

        return fileContent.getInputStream();
    }

    /**
     * Converts a {@link Properties} object to a string-to-string {@link Map}.
     *
     * @param properties Properties
     * @return String-to-string map
     */
    public static Map<String, String> toMap(final Properties properties) {
        return new AbstractMap<String, String>() {
            @SuppressWarnings({"unchecked"})
            public Set<Entry<String, String>> entrySet() {
                return (Set) properties.entrySet();
            }
        };
    }
    /**
     * Replaces tokens in a string.
     *
     * <p>Replaces the tokens "${key}" if "key" occurs in the key-value map.
     * Otherwise "${key}" is left in the string unchanged.
     *
     * @param text Source string
     * @param env Map of key-value pairs
     * @return String with tokens substituted
     */
    public static String replaceProperties(
        String text,
        Map<String, String> env)
    {
        // As of JDK 1.5, cannot use StringBuilder - appendReplacement requires
        // the antediluvian StringBuffer.
        StringBuffer buf = new StringBuffer(text.length() + 200);

        Pattern pattern = Pattern.compile("\\$\\{([^${}]+)\\}");
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            String varName = matcher.group(1);
            String varValue = env.get(varName);
            if (varValue != null) {
                matcher.appendReplacement(buf, varValue);
            } else {
                matcher.appendReplacement(buf, "\\${$1}");
            }
        }
        matcher.appendTail(buf);

        return buf.toString();
    }

    public static String printMemory() {
        return printMemory(null);
    }

    public static String printMemory(String msg) {
        final Runtime rt = Runtime.getRuntime();
        final long freeMemory = rt.freeMemory();
        final long totalMemory = rt.totalMemory();
        final StringBuilder buf = new StringBuilder(64);

        buf.append("FREE_MEMORY:");
        if (msg != null) {
            buf.append(msg);
            buf.append(':');
        }
        buf.append(' ');
        buf.append(freeMemory / 1024);
        buf.append("kb ");

        long hundredths = (freeMemory * 10000) / totalMemory;

        buf.append(hundredths / 100);
        hundredths %= 100;
        if (hundredths >= 10) {
            buf.append('.');
        } else {
            buf.append(".0");
        }
        buf.append(hundredths);
        buf.append('%');

        return buf.toString();
    }

    /**
     * Casts a Set to a Set with a different element type.
     *
     * @param set Set
     * @return Set of desired type
     */
    @SuppressWarnings({"unchecked"})
    public static <T> Set<T> cast(Set<?> set) {
        return (Set<T>) set;
    }

    /**
     * Casts a List to a List with a different element type.
     *
     * @param list List
     * @return List of desired type
     */
    @SuppressWarnings({"unchecked"})
    public static <T> List<T> cast(List<?> list) {
        return (List<T>) list;
    }

    /**
     * Casts a collection to iterable.
     *
     * Under JDK 1.4, {@link Collection} objects do not implement
     * {@link Iterable}, so this method inserts a casting wrapper. (Since
     * Iterable does not exist under JDK 1.4, they will have been compiled
     * under JDK 1.5 or later, then retrowoven to 1.4 class format. References
     * to Iterable will have been replaced with references to
     * <code>com.rc.retroweaver.runtime.Retroweaver_</code>.
     *
     * <p>Under later JDKs this method is trivial. This method can be deleted
     * when we discontinue support for JDK 1.4.
     *
     * @param iterable Object which ought to be iterable
     * @param <T> Element type
     * @return Object cast to Iterable
     */
    public static <T> Iterable<T> castToIterable(
        final Object iterable)
    {
        if (Util.Retrowoven
            && !(iterable instanceof Iterable))
        {
            return new Iterable<T>() {
                public Iterator<T> iterator() {
                    return ((Collection<T>) iterable).iterator();
                }
            };
        }
        return (Iterable<T>) iterable;
    }

    /**
     * Looks up an enumeration by name, returning null if not valid.
     */
    public static <E extends Enum<E>> E lookup(Class<E> clazz, String name) {
        return lookup(clazz, name, null);
    }

    /**
     * Looks up an enumeration by name, returning a given default value if not
     * valid.
     */
    public static <E extends Enum<E>> E lookup(
        Class<E> clazz, String name, E defaultValue)
    {
        try {
            return Enum.valueOf(clazz, name);
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }

    /**
     * Equivalent to {@link java.util.EnumSet#of(Enum, Enum[])} on JDK 1.5 or
     * later. Otherwise, returns an ordinary set.
     *
     * @param first an element that the set is to contain initially
     * @param rest the remaining elements the set is to contain initially
     * @throws NullPointerException if any of the specified elements are null,
     *     or if <tt>rest</tt> is null
     * @return an enum set initially containing the specified elements
     */
    public static <E extends Enum<E>> Set<E> enumSetOf(E first, E... rest) {
        return compatible.enumSetOf(first, rest);
    }

    /**
     * Equivalent to {@link java.util.EnumSet#noneOf(Class)} on JDK 1.5 or
     * later. Otherwise, returns an ordinary set.
     *
     * @param elementType the class object of the element type for this enum
     *     set
     */
    public static <E extends Enum<E>> Set<E> enumSetNoneOf(
        Class<E> elementType)
    {
        return compatible.enumSetNoneOf(elementType);
    }

    /**
     * Equivalent to {@link java.util.EnumSet#allOf(Class)} on JDK 1.5 or later.
     * Otherwise, returns an ordinary set.

     * @param elementType the class object of the element type for this enum
     *     set
     */
    public static <E extends Enum<E>> Set<E> enumSetAllOf(
        Class<E> elementType)
    {
        return compatible.enumSetAllOf(elementType);
    }

    /**
     * Make a BigDecimal from a double. On JDK 1.5 or later, the BigDecimal
     * precision reflects the precision of the double while with JDK 1.4
     * this is not the case.
     *
     * @param d the input double
     * @return the BigDecimal
     */
    public static BigDecimal makeBigDecimalFromDouble(double d) {
        return compatible.makeBigDecimalFromDouble(d);
    }

    /**
     * Returns a literal pattern String for the specified String.
     *
     * <p>Specification as for {@link Pattern#quote(String)}, which was
     * introduced in JDK 1.5.
     *
     * @param s The string to be literalized
     * @return A literal string replacement
     */
    public static String quotePattern(String s) {
        return compatible.quotePattern(s);
    }

    /**
     * Creates a new udf instance from the given udf class.
     *
     * @param udfClass the class to create new instance for
     * @param functionName Function name, or null
     * @return an instance of UserDefinedFunction
     */
    public static UserDefinedFunction createUdf(
        Class<? extends UserDefinedFunction> udfClass,
        String functionName)
    {
        // Instantiate class with default constructor.
        UserDefinedFunction udf;
        String className = udfClass.getName();
        String functionNameOrEmpty =
            functionName == null
                ? ""
                : functionName;

        // Find a constructor.
        Constructor<?> constructor;
        Object[] args = {};

        // 0. Check that class is public and top-level or static.
        if (!Modifier.isPublic(udfClass.getModifiers())
            || (udfClass.getEnclosingClass() != null
                && !Modifier.isStatic(udfClass.getModifiers())))
        {
            throw MondrianResource.instance().UdfClassMustBePublicAndStatic.ex(
                functionName,
                className);
        }

        // 1. Look for a constructor "public Udf(String name)".
        try {
            constructor = udfClass.getConstructor(String.class);
            if (Modifier.isPublic(constructor.getModifiers())) {
                args = new Object[] {functionName};
            } else {
                constructor = null;
            }
        } catch (NoSuchMethodException e) {
            constructor = null;
        }
        // 2. Otherwise, look for a constructor "public Udf()".
        if (constructor == null) {
            try {
                constructor = udfClass.getConstructor();
                if (Modifier.isPublic(constructor.getModifiers())) {
                    args = new Object[] {};
                } else {
                    constructor = null;
                }
            } catch (NoSuchMethodException e) {
                constructor = null;
            }
        }
        // 3. Else, no constructor suitable.
        if (constructor == null) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionNameOrEmpty,
                className,
                UserDefinedFunction.class.getName());
        }
        // Instantiate class.
        try {
            udf = (UserDefinedFunction) constructor.newInstance(args);
        } catch (InstantiationException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionNameOrEmpty,
                className, UserDefinedFunction.class.getName());
        } catch (IllegalAccessException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionName,
                className,
                UserDefinedFunction.class.getName());
        } catch (ClassCastException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionNameOrEmpty,
                className,
                UserDefinedFunction.class.getName());
        } catch (InvocationTargetException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionName,
                className,
                UserDefinedFunction.class.getName());
        }

        return udf;
    }

    /**
     * Check the resultSize against the result limit setting. Throws
     * LimitExceededDuringCrossjoin exception if limit exceeded.
     *
     * When it is called from RolapNativeSet.checkCrossJoin(), it is only
     * possible to check the known input size, because the final CJ result
     * will come from the DB(and will be checked against the limit when
     * fetching from the JDBC result set, in SqlTupleReader.prepareTuples())
     *
     * @param resultSize Result limit
     * @throws ResourceLimitExceededException
     */
    public static void checkCJResultLimit(long resultSize) {
        int resultLimit = MondrianProperties.instance().ResultLimit.get();

        // Throw an exeption, if the size of the crossjoin exceeds the result
        // limit.
        if (resultLimit > 0 && resultLimit < resultSize) {
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                resultSize, resultLimit);
        }

        // Throw an exception if the crossjoin exceeds a reasonable limit.
        // (Yes, 4 billion is a reasonable limit.)
        if (resultSize > Integer.MAX_VALUE) {
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                resultSize, Integer.MAX_VALUE);
        }
    }
}

// End Util.java
