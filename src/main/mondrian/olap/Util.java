/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2006 Julian Hyde and others
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
import java.io.BufferedReader;
import java.io.Reader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;

import mondrian.olap.fun.*;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.mdx.*;

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
     *
     * <p>If this variable is true, we will be running retroweaver. Retroweaver
     * has some problems involving {@link java.util.EnumSet}.
     */
    private static final boolean PreJdk15 =
        System.getProperty("java.version").startsWith("1.4");

    public static boolean isNull(Object o) {
        return o == null || o == nullValue;
    }


    /**
     * Encodes string for MDX (escapes ] as ]] inside a name).
     */
    public static String mdxEncodeString(String st) {
        StringBuilder retString = new StringBuilder(st.length() + 20);
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
        StringBuilder buf = new StringBuilder(val.length()+20);
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
    public static String quoteMdxIdentifier(String[] ids) {
        StringBuilder sb = new StringBuilder(64);
        quoteMdxIdentifier(ids, sb);
        return sb.toString();
    }

    public static void quoteMdxIdentifier(String[] ids, StringBuilder sb) {
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
        boolean caseSensitive = MondrianProperties.instance().CaseSensitive.get();
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
     * Compares two names.
     * Takes into account the {@link MondrianProperties#CaseSensitive case
     * sensitive option}.
     * Names must not be null.
     */
    public static int compareName(String s, String t) {
        boolean caseSensitive = MondrianProperties.instance().CaseSensitive.get();
        return caseSensitive ? s.compareTo(t) : s.compareToIgnoreCase(t);
    }

    /**
     * Generates a normalized form of a name, for use as a key into a map.
     * Returns the upper case name if
     * {@link MondrianProperties#CaseSensitive} is true, the name unchanged
     * otherwise.
     */
    public static String normalizeName(String s) {
        return MondrianProperties.instance().CaseSensitive.get() ?
                s :
                s.toUpperCase();
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
     * @param start Ordinal within <code>find</code> to start searching
     * @param find String to find
     * @param replace String to replace it with
     * @return The string buffer
     */
    public static StringBuilder replace(
            StringBuilder buf,
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
        if (!s.startsWith("[")) {
            return new String[]{s};
        }
        List<String> list = new ArrayList<String>();
        int i = 0;
        while (i < s.length()) {
            if (s.charAt(i) != '[') {
                throw MondrianResource.instance().MdxInvalidMember.ex(s);
            }
            // s may contain extra ']' characters, so look for a ']' followed
            // by a '.'
            int j = s.indexOf("].", i);
            if (j == -1) {
                j = s.lastIndexOf(']');
            }
            if (j <= i) {
                throw MondrianResource.instance().MdxInvalidMember.ex(s);
            }
            String sub = s.substring(i + 1, j);
            sub = replace(sub, "]]", "]");
            list.add(sub);
            if (j + 1 < s.length()) {
                if (s.charAt(j + 1) != '.') {
                    throw MondrianResource.instance().MdxInvalidMember.ex(s);
                }
            }
            i = j +  2;
        }
        return (String[]) list.toArray(new String[list.size()]);
    }

    /**
     * Converts an array of name parts {"part1", "part2"} into a single string
     * "[part1].[part2]". If the names contain "]" they are escaped as "]]".
     */
    public static String implode(String[] names) {
        StringBuilder sb = new StringBuilder(64);
        for (int i = 0; i < names.length; i++) {
            if (i > 0) {
                sb.append(".");
            }
            quoteMdxIdentifier(names[i], sb);
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
        String[] names,
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
     * @param category Type of returned element, a {@link mondrian.olap.Category} value;
     *   {@link mondrian.olap.Category#Unknown} if it doesn't matter.
     * @pre parent != null
     * @post !(failIfNotFound && return == null)
     * @see #explode
     */
    public static OlapElement lookupCompound(
        SchemaReader schemaReader,
        OlapElement parent,
        String[] names,
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
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            OlapElement child =
                schemaReader.getElementChild(parent, name, matchType);
            // if we're doing a non-exact match, and we find a non-exact
            // match, then for an after match, return the first child
            // of each subsequent level; for a before match, return the
            // last child
            if (child != null && matchType != MatchType.EXACT &&
                !Util.equalName(child.getName(), name))
            {
                Util.assertPrecondition(child instanceof Member);
                Member bestChild = (Member) child;
                for (int j = i + 1; j < names.length; j++) {
                    Member[] children =
                        schemaReader.getMemberChildren(bestChild);
                    List<Member> childrenList = Arrays.asList(children);
                    FunUtil.hierarchize(childrenList, false);
                    if (matchType == MatchType.AFTER) {
                        bestChild = childrenList.get(0);
                    } else {
                        bestChild =
                            childrenList.get(children.length - 1);
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
                    StringBuilder buf = new StringBuilder(64);
                    buf.append("Util.lookupCompound: ");
                    buf.append("parent.name=");
                    buf.append(parent.getName());
                    buf.append(" has no child with name=");
                    buf.append(name);
                    LOGGER.debug(buf.toString());
                }

                if (failIfNotFound) {
                    throw MondrianResource.instance().MdxChildObjectNotFound.ex(
                        name, parent.getQualifiedName());
                } else {
                    return null;
                }
            }
            parent = child;
        }
        if (LOGGER.isDebugEnabled()) {
            StringBuilder buf = new StringBuilder(64);
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
                throw MondrianResource.instance().MdxCantFindMember.ex(implode(names));
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
            Query q, String[] nameParts, boolean allowProp) {

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
            if (allowProp &&
                    nameParts.length > 1) {
                String[] namePartsButOne = new String[nameParts.length - 1];
                System.arraycopy(nameParts, 0,
                        namePartsButOne, 0,
                        nameParts.length - 1);
                final String propertyName = nameParts[nameParts.length - 1];
                olapElement = schemaReader.lookupCompound(
                        q.getCube(), namePartsButOne, false, Category.Member);
                if (olapElement != null &&
                        isValidProperty((Member) olapElement, propertyName)) {
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
                int nameLen = nameParts.length - 1;
                olapElement = null;
                while (nameLen > 0 && olapElement == null) {
                    String[] partialName = new String[nameLen];
                    System.arraycopy(
                        nameParts,
                        0,
                        partialName,
                        0,
                        nameLen);
                    olapElement = schemaReader.lookupCompound(
                        q.getCube(), partialName, false, Category.Unknown);
                    nameLen--;
                }
                if (olapElement != null) {
                    olapElement = olapElement.getHierarchy().getNullMember();
                } else {
                    throw MondrianResource.instance().MdxChildObjectNotFound.ex(
                        fullName, q.getCube().getQualifiedName());
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
        SchemaReader reader, Hierarchy hierarchy, String memberName)
    {
        return lookupHierarchyRootMember(
            reader, hierarchy, memberName, MatchType.EXACT);
    }

    /**
     * Finds a root member of a hierarchy with a given name.
     *
     * @param hierarchy
     * @param memberName
     * @return Member, or null if not found
     */
    public static Member lookupHierarchyRootMember(
        SchemaReader reader,
        Hierarchy hierarchy,
        String memberName,
        MatchType matchType)
    {
        // Lookup member at first level.
        Member[] rootMembers = reader.getHierarchyRootMembers(hierarchy);
        
        // if doing an inexact search on a non-all hieararchy, create
        // a member corresponding to the name we're searching for so
        // we can use it in a hierarchical search
        Member searchMember = null;
        if (matchType != MatchType.EXACT && !hierarchy.hasAll() &&
            rootMembers.length > 0)
        {
            searchMember =
                hierarchy.createMember(
                    null,
                    rootMembers[0].getLevel(),
                    memberName,
                    null);
        }
        
        int bestMatch = -1;
        for (int i = 0; i < rootMembers.length; i++) {
            int rc;
            // when searching on the ALL hierarchy, match must be exact
            if (matchType == MatchType.EXACT || hierarchy.hasAll()) {
                rc = rootMembers[i].getName().compareToIgnoreCase(memberName);
            } else {
                rc = FunUtil.compareSiblingMembers(
                    rootMembers[i],
                    searchMember);
            }
            if (rc == 0) {
                return rootMembers[i];
            }
            if (!hierarchy.hasAll()) {
                if (matchType == MatchType.BEFORE) {
                    if (rc < 0 &&
                        (bestMatch == -1 ||
                        FunUtil.compareSiblingMembers(
                            rootMembers[i], 
                            rootMembers[bestMatch]) > 0))
                    {
                        bestMatch = i;
                    }
                } else if (matchType == MatchType.AFTER) {
                    if (rc > 0 &&
                        (bestMatch == -1 ||
                        FunUtil.compareSiblingMembers(
                            rootMembers[i],
                            rootMembers[bestMatch]) < 0))
                    {
                        bestMatch = i;
                    }
                }           
            }
        }
        if (matchType != MatchType.EXACT && bestMatch != -1) {
            return rootMembers[bestMatch];
        }
        // If the first level is 'all', lookup member at second level. For
        // example, they could say '[USA]' instead of '[(All
        // Customers)].[USA]'.
        return (rootMembers.length == 1 && rootMembers[0].isAll())
            ? reader.lookupMemberChildByName(
                rootMembers[0],
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
    public static Member getFirstDescendantOnLevel(
        SchemaReader reader,
        Member parent,
        Level level)
    {
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
            Member member, String propertyName) {
        return lookupProperty(member.getLevel(), propertyName) != null;
    }

    /**
     * Finds a member property called <code>propertyName</code> at, or above,
     * <code>level</code>.
     */
    protected static Property lookupProperty(Level level, String propertyName) {
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
        if (property != null &&
                property.isMemberProperty() &&
                property.isStandard()) {
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

    public static Member[] addLevelCalculatedMembers(
            SchemaReader reader,
            Level level,
            Member[] members) {
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
                new ArrayList<Member>(Arrays.asList(members));
            newMemberList.addAll(calcMembersInThisLevel);
            members = newMemberList.toArray(new Member[newMemberList.size()]);
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
        return Util.newInternal("Was not expecting value '" + anEnum +
            "' for enumeration '" + anEnum.getDeclaringClass().getName() +
            "' in this context");
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
    public static class PropertyList {
        List<String[]> list = new ArrayList<String[]>();

        public String get(String key) {
            return get(key, null);
        }

        public String get(String key, String defaultValue) {
            for (int i = 0, n = list.size(); i < n; i++) {
                String[] pair = list.get(i);
                if (pair[0].equalsIgnoreCase(key)) {
                    return pair[1];
                }
            }
            return defaultValue;
        }

        public String put(String key, String value) {
            for (int i = 0, n = list.size(); i < n; i++) {
                String[] pair = list.get(i);
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
            StringBuilder sb = new StringBuilder(64);
            for (int i = 0, n = list.size(); i < n; i++) {
                String[] pair = list.get(i);
                if (i > 0) {
                    sb.append("; ");
                }
                sb.append(pair[0]);
                sb.append('=');

                if (pair[1] == null) {
                    sb.append("'null'");
                } else {
                    /*
                     * Quote a property value if is has a semi colon in it
                     * 'xxx;yyy';
                     */
                    if (pair[1].indexOf(';') >= 0 && pair[1].charAt(0) != '\'') {
                        sb.append("'");
                    }

                    sb.append(pair[1]);

                    if (pair[1].indexOf(';') >= 0 && pair[1].charAt(pair[1].length() - 1) != '\'') {
                        sb.append("'");
                    }
                }

            }
            return sb.toString();
        }

        public Iterator<String[]> iterator() {
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

            public boolean canConvert(Exp fromExp, int to, int[] conversionCount) {
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
                String description) {
                return null;
            }
        };
    }

    /**
     * Read a Reader until EOF and return as String.
     * Note: this ought to be in a Utility class.
     *
     * @param rdr  Reader to Read.
     * @param bufferSize size of buffer to allocate for reading.
     * @return content of Reader as String or null if Reader was empty.
     * @throws IOException
     */
    public static String readFully(final Reader rdr, final int bufferSize)
            throws IOException {

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
            throws MalformedURLException, IOException {
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
    public static String readURL(final String urlStr, Map map)
            throws MalformedURLException, IOException {
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
    public static String readURL(final URL url, Map<String, String> map) throws IOException {
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

    public static Map<String, String> toMap(final Properties properties) {
        return new AbstractMap<String, String>() {
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
     * Returns whether an enumeration value is a valid not-null value of a given 
     * enumeration class.
     *
     * @param clazz Enumeration class
     * @param e Enumeration value
     * @return Whether t is a value of enum clazz
     */
    public static <E extends Enum<E>> boolean isValid(Class<E> clazz, E e) {
        E[] enumConstants = clazz.getEnumConstants();
        for (E enumConstant : enumConstants) {
            if (e == enumConstant) {
                return true;
            }
        }
        return false;
    }

    /**
     * Looks up an enumeration by name, returns null if not valid.
     */
    public static <E extends Enum<E>> E lookup(Class<E> clazz, String name) {
        try {
            return Enum.valueOf(clazz, name);
        } catch (IllegalArgumentException e) {
            return null;
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
        if (PreJdk15) {
            HashSet<E> set = new HashSet<E>();
            set.add(first);
            for (E e : rest) {
                set.add(e);
            }
            return set;
        } else {
            try {
                Class clazz = Class.forName("java.util.EnumSet");
                Method method = clazz.getMethod("of", Enum.class, Enum[].class);
                return (Set<E>) method.invoke(null, first, rest);
            } catch (ClassNotFoundException e) {
                throw Util.newError(e, "while invoking EnumSet.of()");
            } catch (NoSuchMethodException e) {
                throw Util.newError(e, "while invoking EnumSet.of()");
            } catch (IllegalAccessException e) {
                throw Util.newError(e, "while invoking EnumSet.of()");
            } catch (InvocationTargetException e) {
                throw Util.newError(e, "while invoking EnumSet.of()");
            }
        }
    }

    /**
     * Equivalent to {@link java.util.EnumSet#noneOf(Class)} on JDK 1.5 or later.
     * Otherwise, returns an ordinary set.

     * @param elementType the class object of the element type for this enum
     *     set
     */
    public static <E extends Enum<E>> Set<E> enumSetNoneOf(Class<E> elementType) {
        if (PreJdk15) {
            return new HashSet<E>();
        } else {
            try {
                Class classEnumSet = Class.forName("java.util.EnumSet");
                Method method = classEnumSet.getMethod("noneOf", Class.class);
                return (Set<E>) method.invoke(null, elementType);
            } catch (ClassNotFoundException e) {
                throw Util.newError(e, "while invoking EnumSet.noneOf()");
            } catch (NoSuchMethodException e) {
                throw Util.newError(e, "while invoking EnumSet.noneOf()");
            } catch (IllegalAccessException e) {
                throw Util.newError(e, "while invoking EnumSet.noneOf()");
            } catch (InvocationTargetException e) {
                throw Util.newError(e, "while invoking EnumSet.noneOf()");
            }
        }
    }

    /**
     * Equivalent to {@link java.util.EnumSet#allOf(Class)} on JDK 1.5 or later.
     * Otherwise, returns an ordinary set.

     * @param elementType the class object of the element type for this enum
     *     set
     */
    public static <E extends Enum<E>> Set<E> enumSetAllOf(Class<E> elementType) {
        if (PreJdk15) {
            return new HashSet<E>(Arrays.asList(elementType.getEnumConstants()));
        } else {
            try {
                Class classEnumSet = Class.forName("java.util.EnumSet");
                Method method = classEnumSet.getMethod("allOf", Class.class);
                return (Set<E>) method.invoke(null, elementType);
            } catch (ClassNotFoundException e) {
                throw Util.newError(e, "while invoking EnumSet.noneOf()");
            } catch (NoSuchMethodException e) {
                throw Util.newError(e, "while invoking EnumSet.noneOf()");
            } catch (IllegalAccessException e) {
                throw Util.newError(e, "while invoking EnumSet.noneOf()");
            } catch (InvocationTargetException e) {
                throw Util.newError(e, "while invoking EnumSet.noneOf()");
            }
        }
    }
}

// End Util.java
