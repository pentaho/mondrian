/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.Hierarchy;

import java.util.List;

/**
 * A <code>MemberReader</code> implements common operations to retrieve members
 * from a hierarchy.
 *
 * <p><code>MemberReader</code> is an extension of {@link MemberSource}, which
 * implements only the very basic operations. {@link CacheMemberReader} is an
 * adapter which converts a {@link MemberSource} into a {@link MemberReader}
 * and does caching too.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
interface MemberReader extends MemberSource
{
    /**
     * Returns the member <code>n</code> after <code>member</code> in the same
     * level (or before, if <code>n</code> is negative).
     * Returns {@link Hierarchy#getNullMember} if we run off the beginning or
     * end of the level.
     **/
    RolapMember getLeadMember(RolapMember member, int n);
    /**
     * Returns all of the members in <code>level</code> whose ordinal lies
     * between <code>startOrdinal</code> and <code>endOrdinal</code>.
     *
     * <p>If this object
     * {@link MemberSource#setCache supports cache-writeback}, also
     * writes these members to the cache.
     *
     * @return {@link List} of {@link RolapMember}
     **/
    List getMembersInLevel(RolapLevel level, int startOrdinal, int endOrdinal);
    /**
     * Writes all members between <code>startMember</code> and
     * <code>endMember</code> into <code>list</code>.
     **/
    void getMemberRange(RolapLevel level, RolapMember startMember,
                        RolapMember endMember, List list);
    /**
     * Compares two members according to their order in a prefix ordered
     * traversal. If <code>siblingsAreEqual</code>, then two members with the
     * same parent will compare equal.
     *
     * @return less than zero if m1 occurs before m2,
     *     greater than zero if m1 occurs after m2,
     *     zero if m1 is equal to m2, or if <code>siblingsAreEqual</code> and
     *         m1 and m2 have the same parent
     */
    int compare(RolapMember m1, RolapMember m2, boolean siblingsAreEqual);

    /**
     * Returns the direct and indirect children of a member.
     *
     * @param member Member whose children to find
     * @param result List to which to append results
     * @param level Level relative to which to write results; must belong to
     *   the same hierarchy as <code>member</code>
     * @param before Whether to output members above <code>level</code>
     * @param self Whether to output members at <code>level</code>
     * @param after Whether to output members below <code>level</code>
     */
    void getMemberDescendants(RolapMember member, List result,
            RolapLevel level, boolean before, boolean self, boolean after);
}

// End MemberReader.java
