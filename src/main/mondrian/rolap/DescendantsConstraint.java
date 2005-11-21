package mondrian.rolap;

import java.util.List;

import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;

/**
 * restricts the result of a LevelMembers sqlQuery to a set of parents.
 * The parents must belong to the same level.
 * @author av
 * @since Nov 10, 2005
 */
class DescendantsConstraint implements TupleConstraint {
    List parentMembers;
    MemberChildrenConstraint mcc;

    /**
     * @param parentMembers list of parents all from the same level
     * @param mcc the constraint that would return the children for each single parent
     */
    public DescendantsConstraint(List parentMembers, MemberChildrenConstraint mcc) {
        this.parentMembers = parentMembers;
        this.mcc = mcc;
    }

    public void addConstraint(SqlQuery sqlQuery) {
        mcc.addMemberConstraint(sqlQuery, parentMembers);
    }

    public void addLevelConstraint(SqlQuery sqlQuery, RolapLevel level) {
        mcc.addLevelConstraint(sqlQuery, level);
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent) {
        return mcc;
    }

    /**
     * returns null, because descendants is not cached.
     */
    public Object getCacheKey() {
        return null;
    }
}
