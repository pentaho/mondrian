/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
//
// jhyde, 10 August, 2001
*/
package mondrian.rolap;

import mondrian.olap.Hierarchy;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

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
 */
public interface MemberReader extends MemberSource {
    /**
     * Returns the member <code>n</code> after <code>member</code> in the same
     * level (or before, if <code>n</code> is negative).
     * Returns {@link Hierarchy#getNullMember} if we run off the beginning or
     * end of the level.
     */
    RolapMember getLeadMember(RolapMember member, int n);

    /**
     * Returns all of the members in <code>level</code> whose ordinal lies
     * between <code>startOrdinal</code> and <code>endOrdinal</code>.
     *
     * <p>If this object
     * {@link MemberSource#setCache supports cache-writeback}, also
     * writes these members to the cache.
     *
     * @param level Level
     * @return {@link List} of {@link RolapMember}
     */
    List<RolapMember> getMembersInLevel(
        RolapCubeLevel level);

    /**
     * Writes all members between <code>startMember</code> and
     * <code>endMember</code> into <code>list</code>.
     */
    void getMemberRange(
        RolapLevel level,
        RolapMember startMember,
        RolapMember endMember,
        List<RolapMember> list);

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
    int compare(
        RolapMember m1,
        RolapMember m2,
        boolean siblingsAreEqual);

    /**
     * Populates a list of the children of a Member, optionally applying a
     * constraint.
     *
     * @param member Members whose children to find
     * @param children List to populate with members
     * @param constraint Constraint
     */
    void getMemberChildren(
        RolapMember member,
        List<RolapMember> children,
        MemberChildrenConstraint constraint);

    /**
     * Populates a list of the children of a given set of Members, optionally
     * applying a constraint.
     *
     * @param parentMembers List of members whose children to find
     * @param children List to populate with members
     * @param constraint Constraint
     */
    void getMemberChildren(
        List<RolapMember> parentMembers,
        List<RolapMember> children,
        MemberChildrenConstraint constraint);

    /**
     * Returns the members in the given Level, optionally applying a constraint.
     *
     * @param level Level
     * @param constraint Constraint
     * @return list of members
     */
    List<RolapMember> getMembersInLevel(
        RolapCubeLevel level,
        TupleConstraint constraint);

    /**
     * Returns the number of members in this level.
     *
     * @param level Level
     * @return number of members in level
     */
    int getLevelMemberCount(RolapCubeLevel level);

    MemberBuilder getMemberBuilder();

    RolapMember getDefaultMember();

    RolapMember getMemberParent(RolapMember member);

    /**
     * Substitutes a given member.
     * If member is null, returns null.
     *
     * <p>This method is called whenever a member is returned from the wrapped
     * member reader and is to be returned to the caller. You could say that it
     * translates 'to caller space'.
     *
     * @param member Member
     * @return Substitute member
     */
    RolapMember substitute(RolapMember member);

    /**
     * Returns the member which was substituted.
     * If member is null, returns null.
     *
     * <p>This method is called whenever the caller passes a member into a
     * method and needs to be passed to a method on the wrapped member reader.
     * You could say that it translates 'from caller space'.
     *
     * @param member Member
     * @return Internal member
     */
    RolapMember desubstitute(RolapMember member);

    /**
     * Looks up a member by its key value.
     *
     * @param level Level
     * @param keyValues Key values
     * @return Member, or null
     */
    RolapMember getMemberByKey(
        RolapCubeLevel level,
        List<Comparable> keyValues);
}

// End MemberReader.java
