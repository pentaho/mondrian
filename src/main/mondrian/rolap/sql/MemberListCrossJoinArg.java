/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2006-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap.sql;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.*;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents an enumeration {member1, member2, ...}.
 * All members must to the same level and are non-calculated.
 */
public class MemberListCrossJoinArg implements CrossJoinArg {
    private final List<RolapMember> members;
    private final RolapLevel level;
    private final boolean restrictMemberTypes;
    private final boolean hasCalcMembers;
    private final boolean hasNonCalcMembers;
    private final boolean hasAllMember;
    private final boolean exclude;

    private MemberListCrossJoinArg(
        RolapLevel level,
        List<RolapMember> members,
        boolean restrictMemberTypes,
        boolean hasCalcMembers,
        boolean hasNonCalcMembers,
        boolean hasAllMember,
        boolean exclude)
    {
        this.level = level;
        this.members = members;
        this.restrictMemberTypes = restrictMemberTypes;
        this.hasCalcMembers = hasCalcMembers;
        this.hasNonCalcMembers = hasNonCalcMembers;
        this.hasAllMember = hasAllMember;
        this.exclude = exclude;
    }

    private static boolean isArgSizeSupported(
        RolapEvaluator evaluator,
        int argSize)
    {
        boolean argSizeNotSupported = false;

        // Note: arg size 0 is accepted as valid CJ argument
        // This is used to push down the "1 = 0" predicate
        // into the emerging CJ so that the entire CJ can
        // be natively evaluated.

        // First check that the member list will not result in a predicate
        // longer than the underlying DB could support.
        if (argSize > MondrianProperties.instance().MaxConstraints.get()) {
            argSizeNotSupported = true;
        }

        return !argSizeNotSupported;
    }


    /**
     * Creates an instance of {@link CrossJoinArg},
     * or returns null if the arguments are invalid. This method also
     * records properties of the member list such as containing
     * calc/non calc members, and containing the All member.
     *
     * <p>If restrictMemberTypes is set, then the resulting argument could
     * contain calculated members. The newly created CrossJoinArg is marked
     * appropriately for special handling downstream.
     *
     * <p>If restrictMemberTypes is false, then the resulting argument
     * contains non-calculated members of the same level (after filtering
     * out any null members).
     *
     * @param evaluator the current evaluator
     * @param args members in the list
     * @param restrictMemberTypes whether calculated members are allowed
     * @param exclude Whether to exclude tuples that match the predicate
     * @return MemberListCrossJoinArg if member list is well formed,
     *   null if not.
     */
    public static CrossJoinArg create(
        RolapEvaluator evaluator,
        final List<RolapMember> args,
        final boolean restrictMemberTypes,
        boolean exclude)
    {
        // First check that the member list will not result in a predicate
        // longer than the underlying DB could support.
        if (!isArgSizeSupported(evaluator, args.size())) {
            return null;
        }

        RolapLevel level = null;
        RolapLevel nullLevel = null;
        boolean hasCalcMembers = false;
        boolean hasNonCalcMembers = false;

        // Crossjoin Arg is an empty member list.
        // This is used to push down the constant "false" condition to the
        // native evaluator.
        if (args.size() == 0) {
            hasNonCalcMembers = true;
        }
        boolean hasAllMember = false;
        for (RolapMember m : args) {
            if (m.isNull()) {
                // we're going to filter out null members anyway;
                // don't choke on the fact that their level
                // doesn't match that of others
                nullLevel = m.getLevel();
                continue;
            }

            // If "All" member, native evaluation is not possible
            // because "All" member does not have a corresponding
            // relational representation.
            //
            // "All" member is ignored during SQL generation.
            // The complete MDX query can be evaluated natively only
            // if there is non all member on at least one level;
            // otherwise the generated SQL is an empty string.
            // See SqlTupleReader.addLevelMemberSql()
            //
            if (m.isAll()) {
                hasAllMember = true;
            }

            if (m.isCalculated() && !m.isParentChildLeaf()) {
                if (restrictMemberTypes) {
                    return null;
                }
                hasCalcMembers = true;
            } else {
                hasNonCalcMembers = true;
            }
            if (level == null) {
                level = m.getLevel();
            } else if (!level.equals(m.getLevel())) {
                // Members should be on the same level.
                return null;
            }
        }
        if (level == null) {
            // all members were null; use an arbitrary one of the
            // null levels since the SQL predicate is going to always
            // fail anyway
            level = nullLevel;
        }

        // level will be null for an empty CJ input that is pushed down
        // to the native evaluator.
        // This case is not treated as a non-native input.
        if ((level != null) && (!level.isSimple()
            && !supportedParentChild(level, args)))
        {
            return null;
        }
        List<RolapMember> members = new ArrayList<RolapMember>();

        for (RolapMember m : args) {
            if (m.isNull()) {
                // filter out null members
                continue;
            }
            members.add(m);
        }

        return new MemberListCrossJoinArg(
            level, members, restrictMemberTypes,
            hasCalcMembers, hasNonCalcMembers, hasAllMember, exclude);
    }

    private static boolean supportedParentChild(
        RolapLevel level, List<RolapMember> args)
    {
        if (level.isParentChild()) {
            boolean allArgsLeaf = true;
            for (RolapMember rolapMember : args) {
            if (!rolapMember.isParentChildLeaf()) {
                allArgsLeaf = false;
                break;
            }
        }
            return allArgsLeaf;
        }
        return false;
    }

    public RolapLevel getLevel() {
        return level;
    }

    public List<RolapMember> getMembers() {
        return members;
    }

    public boolean isPreferInterpreter(boolean joinArg) {
        if (joinArg) {
            // If this enumeration only contains calculated members,
            // prefer non-native evaluation.
            return hasCalcMembers && !hasNonCalcMembers;
        } else {
            // For non-join usage, always prefer non-native
            // eval, since the members are already known.
            return true;
        }
    }

    public void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar)
    {
        SqlConstraintUtils.addMemberConstraint(
            sqlQuery, baseCube, aggStar,
            members, restrictMemberTypes, true, exclude);
    }

    /**
     * Returns whether the input CJ arg is empty.
     *
     * <p>This is used to selectively push down empty input arg into the
     * native evaluator.
     *
     * @return whether the input CJ arg is empty
     */
    public boolean isEmptyCrossJoinArg() {
        return (level == null && members.size() == 0);
    }

    public boolean hasCalcMembers() {
        return hasCalcMembers;
    }

    public boolean hasAllMember() {
        return hasAllMember;
    }

    public int hashCode() {
        int c = 12;
        for (RolapMember member : members) {
            c = 31 * c + member.hashCode();
        }
        if (restrictMemberTypes) {
            c += 1;
        }
        if (exclude) {
            c += 7;
        }
        return c;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof MemberListCrossJoinArg)) {
            return false;
        }
        MemberListCrossJoinArg that = (MemberListCrossJoinArg) obj;
        if (this.restrictMemberTypes != that.restrictMemberTypes) {
            return false;
        }
        if (this.exclude != that.exclude) {
            return false;
        }
        for (int i = 0; i < members.size(); i++) {
            if (this.members.get(i) != that.members.get(i)) {
                return false;
            }
        }
        return true;
    }
}

// End MemberListCrossJoinArg.java
