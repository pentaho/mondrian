/*
 //This software is subject to the terms of the Common Public License
 //Agreement, available at the following URL:
 //http://www.opensource.org/licenses/cpl.html.
 //Copyright (C) 2004-2005 TONBELLER AG
 //All Rights Reserved.
 //You must accept the terms of that agreement to use this software.
 */
package mondrian.rolap;

import java.util.List;

import mondrian.olap.Evaluator;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;

/**
 * creates the right constraint for common tasks.
 * 
 * @author av
 * @since Nov 21, 2005
 */
public class SqlConstraintFactory {

    private static final SqlConstraintFactory instance = new SqlConstraintFactory();

    /** singleton */
    private SqlConstraintFactory() {
    }

    public static final SqlConstraintFactory instance() {
        return instance;
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(Evaluator context) {
        if (context == null)
            return DefaultMemberChildrenConstraint.instance();
        return new SqlContextConstraint((RolapEvaluator) context, false);
    }

    public TupleConstraint getLevelMembersConstraint(Evaluator context) {
        if (context == null)
            return DefaultTupleConstraint.instance();
        return new SqlContextConstraint((RolapEvaluator) context, false);
    }

    public MemberChildrenConstraint getChildByNameConstraint(RolapMember parent,
            String childName) {
        // ragged hierarchies span multiple levels, so SQL WHERE does not work there
        if (parent.getRolapHierarchy().isRagged())
            return DefaultMemberChildrenConstraint.instance();
        return new ChildByNameConstraint(childName);
    }

    /**
     * returns a constraint that allows to read all children of multiple parents at once
     * using a LevelMember query style. This does not work for parent/child hierarchies.
     */
    public TupleConstraint getDescendantsConstraint(List parentMembers,
            MemberChildrenConstraint mcc) {
        return new DescendantsConstraint(parentMembers, mcc);
    }

}
