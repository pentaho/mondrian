package mondrian.rolap.sql;

import mondrian.rolap.RolapCube;
import mondrian.rolap.RolapLevel;
import mondrian.rolap.RolapMember;
import mondrian.rolap.aggmatcher.AggStar;

import java.util.List;

/**
     * "Light version" of a {@link mondrian.rolap.sql.TupleConstraint}, represents one of
 * member.children, level.members, member.descendants, {enumeration}.
 */
public interface CrossJoinArg {
    CrossJoinArg[] EMPTY_ARRAY = new CrossJoinArg[0];

    RolapLevel getLevel();

    List<RolapMember> getMembers();

    void addConstraint(
        SqlQuery sqlQuery,
        RolapCube baseCube,
        AggStar aggStar);

    boolean isPreferInterpreter(boolean joinArg);
}

// End CrossJoinArg.java
