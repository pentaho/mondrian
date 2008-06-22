/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 24, 2003
*/
package mondrian.olap;

import mondrian.calc.Calc;

import javax.sql.DataSource;
import java.util.List;

/**
 * A <code>SchemaReader</code> queries schema objects ({@link Schema},
 * {@link Cube}, {@link Dimension}, {@link Hierarchy}, {@link Level},
 * {@link Member}).
 *
 * <p>It is generally created using {@link Connection#getSchemaReader}.
 *
 * @author jhyde
 * @since Feb 24, 2003
 * @version $Id$
 */
public interface SchemaReader {
    /**
     * Returns the access-control profile that this <code>SchemaReader</code>
     * is implementing.
     */
    Role getRole();

    /**
     * Returns an array of the root members of <code>hierarchy</code>.
     *
     * @param hierarchy Hierarchy
     * @see #getCalculatedMembers(Hierarchy)
     */
    Member[] getHierarchyRootMembers(Hierarchy hierarchy);

    /**
     * Returns number of children parent of a member,
     *  if the information can be retrieved from cache.
     * Otherwise  -1 is returned
     */
    int getChildrenCountFromCache(Member member);

    /**
     * Returns the number of members in a level, returning an approximation if
     * acceptable.
     *
     * @param level Level
     * @param approximate Whether an approximation is acceptable
     * @param materialize Whether to go to disk if no approximation for the count
     *   is available and the members are not in cache. If false, returns
     *   {@link Integer#MIN_VALUE} if value is not in cache.
     */
    int getLevelCardinality(
        Level level, boolean approximate, boolean materialize);

    /**
     * Substitutes a member with an equivalent member which enforces the
     * access control policy of this SchemaReader.
     */
    Member substitute(Member member);

    /**
     * Returns direct children of <code>member</code>.
     * @pre member != null
     * @post return != null
     */
    Member[] getMemberChildren(Member member);

    /**
     * Returns direct children of <code>member</code>, optimized
     * for NON EMPTY.
     * <p>
     * If <code>context == null</code> then
     * there is no context and all members are returned - then
     * its identical to {@link #getMemberChildren(Member)}.
     * If <code>context</code> is not null, the resulting members
     * <em>may</em> be restricted to those members that have a
     * non empty row in the fact table for <code>context</code>.
     * Wether or not optimization is possible depends
     * on the SchemaReader implementation.
     */
    Member[] getMemberChildren(Member member, Evaluator context);

    /**
     * Returns direct children of each element of <code>members</code>.
     *
     * @param members Array of members
     * @return array of child members
     *
     * @pre members != null
     * @post return != null
     */
    Member[] getMemberChildren(Member[] members);

    /**
     * Returns direct children of each element of <code>members</code>
     * which is not empty in <code>context</code>.
     *
     * @param members Array of members
     * @param context Evaluation context
     * @return array of child members
     *
     * @pre members != null
     * @post return != null
     */
    Member[] getMemberChildren(Member[] members, Evaluator context);

    /**
     * Returns the parent of <code>member</code>.
     *
     * @param member Member
     * @pre member != null
     * @return null if member is a root member
     */
    Member getMemberParent(Member member);


    /**
     * Returns the depth of a member.
     *
     * <p>This may not be the same as
     * <code>member.{@link Member#getLevel getLevel}().
     * {@link Level#getDepth getDepth}()</code>
     * for three reasons:<ol>
     * <li><b>Access control</b>. The most senior <em>visible</em> member has
     *   level 0. If the client is not allowed to see the "All" and "Nation"
     *   levels of the "Store" hierarchy, then members of the "State" level will
     *   have depth 0.</li>
     * <li><b>Parent-child hierarchies</b>. Suppose Fred reports to Wilma, and
     *   Wilma reports to no one. "All Employees" has depth 0, Wilma has depth
     *   1, and Fred has depth 2. Fred and Wilma are both in the "Employees"
     *   level, which has depth 1.</li>
     * <li><b>Ragged hierarchies</b>. If Israel has only one, hidden, province
     * then the depth of Tel Aviv, Israel is 2, whereas the depth of another
     * city, San Francisco, CA, USA is 3.</li>
     * </ol>
     */
    int getMemberDepth(Member member);

    /**
     * Finds a member based upon its unique name.
     *
     * @param uniqueNameParts Unique name of member
     * @param failIfNotFound Whether to throw an error, as opposed to returning
     *   <code>null</code>, if there is no such member.
     * @param matchType indicates the match mode; if not specified, EXACT
     * @return The member, or null if not found
     */
    Member getMemberByUniqueName(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound,
        MatchType matchType);

    /**
     * Finds a member based upon its unique name, requiring an exact match.
     *
     * <p>This method is equivalent to calling
     * {@link #getMemberByUniqueName(java.util.List, boolean, MatchType)}
     * with {@link MatchType#EXACT}.
     *
     * @param uniqueNameParts Unique name of member
     * @param failIfNotFound Whether to throw an error, as opposed to returning
     *   <code>null</code>, if there is no such member.
     * @return The member, or null if not found
     */
    Member getMemberByUniqueName(
        List<Id.Segment> uniqueNameParts,
        boolean failIfNotFound);

    /**
     * Looks up an MDX object by name, specifying how to
     * match if no object exactly matches the name.
     *
     * <p>Resolves a name such as
     * '[Products]&#46;[Product Department]&#46;[Produce]' by resolving the
     * components ('Products', and so forth) one at a time.
     *
     * @param parent Parent element to search in
     * @param names Exploded compound name, such as {"Products",
     *     "Product Department", "Produce"}
     * @param failIfNotFound If the element is not found, determines whether
     *      to return null or throw an error
     * @param category Type of returned element, a {@link Category} value;
     *      {@link Category#Unknown} if it doesn't matter.
     * @param matchType indicates the match mode; if not specified, EXACT
     *
     * @pre parent != null
     * @post !(failIfNotFound && return == null)
     */
    OlapElement lookupCompound(
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category,
        MatchType matchType);

    /**
     * Looks up an MDX object by name.
     *
     * <p>Resolves a name such as
     * '[Products]&#46;[Product Department]&#46;[Produce]' by resolving the
     * components ('Products', and so forth) one at a time.
     *
     * @param parent Parent element to search in
     * @param names Exploded compound name, such as {"Products",
     *     "Product Department", "Produce"}
     * @param failIfNotFound If the element is not found, determines whether
     *      to return null or throw an error
     * @param category Type of returned element, a {@link Category} value;
     *      {@link Category#Unknown} if it doesn't matter.
     *
     * @pre parent != null
     * @post !(failIfNotFound && return == null)
     */
    OlapElement lookupCompound(
        OlapElement parent,
        List<Id.Segment> names,
        boolean failIfNotFound,
        int category);


    /**
     * Looks up a calculated member by name. If the name is not found in the
     * current scope, returns null.
     */
    Member getCalculatedMember(List<Id.Segment> nameParts);

    /**
     * Looks up a set by name. If the name is not found in the current scope,
     * returns null.
     */
    NamedSet getNamedSet(List<Id.Segment> nameParts);

    /**
     * Appends to <code>list</code> all members between <code>startMember</code>
     * and <code>endMember</code> (inclusive) which belong to
     * <code>level</code>.
     */
    void getMemberRange(
        Level level, Member startMember, Member endMember, List<Member> list);

    /**
     * Returns a member <code>n</code> further along in the same level from
     * <code>member</code>.
     *
     * @pre member != null
     */
    Member getLeadMember(Member member, int n);

    /**
     * Compares a pair of {@link Member}s according to their order in a prefix
     * traversal. (that is, it
     * is an ancestor or a earlier ), is a sibling, or comes later in a prefix
     * traversal.
     * @return A negative integer if <code>m1</code> is an ancestor, an earlier
     *   sibling of an ancestor, or a descendent of an earlier sibling, of
     *   <code>m2</code>;
     *   zero if <code>m1</code> is a sibling of <code>m2</code>;
     *   a positive integer if <code>m1</code> comes later in the prefix
     *   traversal then <code>m2</code>.
     */
    int compareMembersHierarchically(Member m1, Member m2);

    /**
     * Looks up the child of <code>parent</code> called <code>s</code>; returns
     * null if no element is found.
     */
    OlapElement getElementChild(
        OlapElement parent, Id.Segment name, MatchType matchType);

    OlapElement getElementChild(OlapElement parent, Id.Segment name);

    /**
     * Returns the members of a level, optionally including calculated members.
     */
    Member[] getLevelMembers(Level level, boolean includeCalculated);

    /**
     * Returns the members of a level, optionally filtering out members which
     * are empty.
     *
     * @param level Level
     * @param context Context for filtering
     * @return Members of this level
     */
    Member[] getLevelMembers(Level level, Evaluator context);

    /**
     * Returns the accessible levels of a hierarchy.
     *
     * @pre hierarchy != null
     * @post return.length >= 1
     */
    Level[] getHierarchyLevels(Hierarchy hierarchy);

    /**
     * Returns the default member of a hierarchy. If the default member is in
     * an inaccessible level, returns the nearest ascendant/descendant member.
     */
    Member getHierarchyDefaultMember(Hierarchy hierarchy);

    /**
     * Returns whether a member has visible children.
     */
    boolean isDrillable(Member member);

    /**
     * Returns whether a member is visible.
     */
    boolean isVisible(Member member);

    /**
     * Returns the list of accessible cubes.
     */
    Cube[] getCubes();

    /**
     * Returns a list of calculated members in a given hierarchy.
     */
    List<Member> getCalculatedMembers(Hierarchy hierarchy);

    /**
     * Returns a list of calculated members in a given level.
     */
    List<Member> getCalculatedMembers(Level level);

    /**
     * Returns the list of calculated members.
     */
    List<Member> getCalculatedMembers();

    /**
     * Finds a child of a member with a given name.
     */
    Member lookupMemberChildByName(
        Member parent, Id.Segment childName, MatchType matchType);

    Member lookupMemberChildByName(Member parent, Id.Segment childName);

    /**
     * Returns an object which can evaluate an expression in native SQL, or
     * null if this is not possible.
     *
     * @param fun Function
     * @param args Arguments to the function
     * @param evaluator Evaluator, provides context
     * @param calc
     */
    NativeEvaluator getNativeSetEvaluator(
            FunDef fun, Exp[] args, Evaluator evaluator, Calc calc);

    /**
     * Returns the definition of a parameter with a given name, or null if not
     * found.
     */
    Parameter getParameter(String name);

    DataSource getDataSource();

}

// End SchemaReader.java
