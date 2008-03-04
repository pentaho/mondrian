/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.List;

import mondrian.olap.Id;
import mondrian.olap.Evaluator;
import mondrian.olap.Level;
import mondrian.olap.MondrianProperties;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.TupleConstraint;

/**
 * Creates the right constraint for common tasks.
 *
 * @author av
 * @since Nov 21, 2005
 */
public class SqlConstraintFactory {

    boolean enabled = MondrianProperties.instance().EnableNativeNonEmpty.get();

    private static final SqlConstraintFactory instance = new SqlConstraintFactory();

    /** singleton */
    private SqlConstraintFactory() {
    }

    public static SqlConstraintFactory instance() {
        return instance;
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(Evaluator context) {
        if (!enabled || !SqlContextConstraint.isValidContext(context))
            return DefaultMemberChildrenConstraint.instance();
        return new SqlContextConstraint((RolapEvaluator) context, false);
    }

    public TupleConstraint getLevelMembersConstraint(Evaluator context) {
        boolean[] satisfied = {false};
        return getLevelMembersConstraint(context, null, satisfied);
    }

    /**
     * Returns a constraint that restricts the members of a level to those that
     * are non-empty in the given context. If the constraint cannot be
     * implemented (say if native constraints are disabled) returns null.
     *
     * @param context Context within which members must be non-empty
     * @param levels levels being referenced in the current context
     * @param satisfied Set to false if constraint does not satisfy non-empty
     * context and caller still has to do it
     * @return Constraint
     */
    public TupleConstraint getLevelMembersConstraint(
        Evaluator context,
        Level [] levels,
        boolean[] satisfied)
    {
        if (context == null) {
            satisfied[0] = true;
            return DefaultTupleConstraint.instance();
        }
        if (!enabled) {
            // Cannot implement constraint in SQL; caller must still implement
            // it
            satisfied[0] = false;
            return DefaultTupleConstraint.instance();
        }
        satisfied[0] = true;
        if (!SqlContextConstraint.isValidContext(context, false, levels)) {
            return DefaultTupleConstraint.instance();
        }
        return new SqlContextConstraint((RolapEvaluator) context, false);
    }

    public MemberChildrenConstraint getChildByNameConstraint(
        RolapMember parent,
        Id.Segment childName)
    {
        // Ragged hierarchies span multiple levels, so SQL WHERE does not work
        // there
        if (!enabled || parent.getHierarchy().isRagged()) {
            return DefaultMemberChildrenConstraint.instance();
        }
        return new ChildByNameConstraint(childName);
    }

    /**
     * Returns a constraint that allows to read all children of multiple parents
     * at once using a LevelMember query style. This does not work
     * for parent/child hierarchies.
     *
     * @param parentMembers List of parents (all must belong to same level)
     * @param mcc           The constraint that would return the children for
     *                      each single parent
     * @return constraint
     */
    public TupleConstraint getDescendantsConstraint(
        List<RolapMember> parentMembers,
        MemberChildrenConstraint mcc)
    {
        return new DescendantsConstraint(parentMembers, mcc);
    }
}

// End SqlConstraintFactory.java
