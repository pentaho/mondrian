/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2003-2004 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 24, 2003
*/
package mondrian.olap;

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
 **/
public interface SchemaReader {
	/**
	 * Returns the access-control profile that this <code>SchemaReader</code>
	 * is implementing.
	 */
	Role getRole();
	/**
	 * Returns an array of the root members of <code>hierarchy</code>. If the
	 * hierarchy is access-controlled, returns the most senior visible members.
	 **/
	Member[] getHierarchyRootMembers(Hierarchy hierarchy);
	/**
	 * Returns the parent of a member, null if the parent is inaccessible.
	 */
	Member getMemberParent(Member member);
	/**
	 * Returns direct children of <code>member</code>.
	 * @pre member != null
	 * @post return != null
	 **/
	Member[] getMemberChildren(Member member);
	/**
	 * Returns direct children of each element of <code>members</code>.
	 * @pre members != null
	 * @post return != null
	 **/
	Member[] getMemberChildren(Member[] members);
	/**
	 * Returns the depth of a member. This may not be the same as
	 * <code>member.{@link Member#getLevel getLevel}().{@link Level#getDepth getDepth}()</code>
	 * for 2 reasons:<ol>
	 * <li><b>Access control</b>. The most senior <em>visible</em> member has
	 *   level 0. If the client is not allowed to see the "All" and "Nation"
	 *   levels of the "Store" hierarchy, then members of the "State" level will
	 *   have depth 0.</li>
	 * <li><b>Parent-child hierarchies</b>. Suppose Fred reports to Wilma, and
	 * Wilma reports to no one. "All Employees" has depth 0, Wilma has depth 1,
	 * and Fred has depth 2. Fred and Wilma are both in the "Employees" level,
	 * which has depth 1.</li>
	 * </ol>
	 */
	int getMemberDepth(Member member);
	/**
	 * Finds a member based upon its unique name.
	 *
	 * @param uniqueNameParts Unique name of member
	 * @param failIfNotFound Whether to throw an error, as opposed to returning
	 *   <code>null</code>, if there is no such member.
	 * @return The member, or null if not found
	 **/
	Member getMemberByUniqueName(String[] uniqueNameParts, boolean failIfNotFound);

	/**
	 * Appends to <code>list</code> all members between <code>startMember</code>
	 * and <code>endMember</code> (inclusive) which belong to
	 * <code>level</code>.
	 */
	void getMemberRange(Level level, Member startMember, Member endMember, List list);

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
	 **/
	int compareMembersHierarchically(Member m1, Member m2);

	/**
	 * Looks up the child of <code>parent</code> called <code>s</code>; returns
	 * null if no element is found.
	 **/
	OlapElement getElementChild(OlapElement parent, String name);

	/**
	 * Returns the members of <code>level</code>.
	 */
	Member[] getLevelMembers(Level level);
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
}

// End SchemaReader.java
