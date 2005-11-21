package mondrian.rolap;

import mondrian.rolap.sql.TupleConstraint;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;

/**
 * stateless - does not restrict the result
 */
public class DefaultTupleConstraint implements TupleConstraint {

    private static final TupleConstraint instance = new DefaultTupleConstraint();

    /** we have no state, so all instances are equal */
    private static final Object cacheKey = new Object();

    protected DefaultTupleConstraint() {
    }

    public void addConstraint(SqlQuery sqlQuery) {
    }

    public void addLevelConstraint(SqlQuery query, RolapLevel level) {
    }

    public MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent) {
        return DefaultMemberChildrenConstraint.instance();
    }

    public String toString() {
        return "DefaultTupelConstraint";
    }

    public Object getCacheKey() {
        return cacheKey;
    }

    public static TupleConstraint instance() {
        return instance;
    }

}
