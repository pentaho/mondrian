/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.*;
import java.sql.*;

import mondrian.calc.ExpCompiler.ResultStyle;
import mondrian.olap.*;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.cache.HardSmartCache;
import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.mdx.*;

import org.apache.log4j.Logger;

import javax.sql.DataSource;

/**
 * Analyses set expressions and executes them in SQL if possible.
 * Supports crossjoin, member.children, level.members and member.descendants -
 * all in non empty mode, i.e. there is a join to the fact table.<p/>
 *
 * TODO: the order of the result is different from the order of the
 * enumeration. Should sort.
 *
 * @author av
 * @since Nov 12, 2005
 * @version $Id$
 */
public abstract class RolapNativeSet extends RolapNative {
    protected static final Logger LOGGER = Logger.getLogger(RolapNativeSet.class);

    private SmartCache<Object, List<List<RolapMember>>> cache =
        new SoftSmartCache<Object, List<List<RolapMember>>>();

    /**
     * Returns whether calculated members should be accepted (and ignored).
     *
     * <p>If true, calc members will be ignored and the computation still will
     * be done in SQL returning more members than requested.
     * If false, expressions containing calculated members will rejected to the
     * interpreter.
     */
    protected abstract boolean isStrict();

    /**
     * Constraint for non empty {crossjoin, member.children,
     * member.descendants, level.members}
     */
    protected static abstract class SetConstraint extends SqlContextConstraint {
        CrossJoinArg[] args;

        SetConstraint(CrossJoinArg[] args, RolapEvaluator evaluator, boolean strict) {
            super(evaluator, strict);
            this.args = args;
        }

        /**
         * if there is a crossjoin, we need to join the fact table - even if the
         * evalutaor context is empty.
         */
        protected boolean isJoinRequired() {
            return args.length > 1 || super.isJoinRequired();
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            Map<String, RolapStar.Table> relationNamesToStarTableMap) {
            super.addConstraint(sqlQuery, levelToColumnMap, 
                relationNamesToStarTableMap);
            for (CrossJoinArg arg : args) {
                // if the cross join argument has calculated members in its
                // enumerated set, ignore the constraint since we won't
                // produce that set through the native sql and instead
                // will simply enumerate through the members in the set
                if (!(arg instanceof MemberListCrossJoinArg) ||
                    !((MemberListCrossJoinArg) arg).hasCalcMembers()) {
                    arg.addConstraint(sqlQuery, levelToColumnMap, 
                        relationNamesToStarTableMap);
                }
            }
        }

        /**
         * returns null to prevent the member/childern from being cached. There exists
         * no valid MemberChildrenConstraint that would fetch those children that were
         * extracted as a side effect from evaluating a non empty crossjoin
         */
        public MemberChildrenConstraint getMemberChildrenConstraint(RolapMember parent) {
            return null;
        }

        /**
         * returns a key to cache the result
         */
        public Object getCacheKey() {
            List<Object> key = new ArrayList<Object>();
            key.add(super.getCacheKey());
            // only add args that will be retrieved through native sql;
            // args that are sets with calculated members aren't executed
            // natively
            for (CrossJoinArg arg : args) {
                if (!(arg instanceof MemberListCrossJoinArg) ||
                    !((MemberListCrossJoinArg) arg).hasCalcMembers()) {
                    key.add(arg);
                }
            }
            return key;
        }
    }

    protected class SetEvaluator implements NativeEvaluator {
        private CrossJoinArg[] args;
        private SchemaReader schemaReader;
        private TupleConstraint constraint;
        private int maxRows = 0;

        public SetEvaluator(
                CrossJoinArg[] args,
                SchemaReader schemaReader,
                TupleConstraint constraint) {
            this.args = args;
            this.schemaReader = schemaReader;
            this.constraint = constraint;
        }

        public Object execute(ResultStyle desiredResultStyle) {
            switch (desiredResultStyle) {
            case ITERABLE :
                return executeIterable();
            case MUTABLE_LIST :
            case LIST :
                return executeList();
            }
            throw ResultStyleException.generate(
                new ResultStyle[] {
                    ResultStyle.ITERABLE,
                    ResultStyle.MUTABLE_LIST,
                    ResultStyle.LIST
                },
                new ResultStyle[] {
                    desiredResultStyle
                }
            );
        }
        protected Object executeIterable() {
            final List list = executeList();
            if (args.length == 1) {
                return new Iterable<Member>() {
                    public Iterator<Member> iterator() {
                        return new Iterator<Member>() {
                            int index = 0;
                            public boolean hasNext() {
                                return (index < list.size());
                            }
                            public Member next() {
                                return (Member) list.get(index++);
                            }
                            public void remove() {
                                throw new UnsupportedOperationException("remove");
                            }
                        };
                    }
                };
            } else {
                return new Iterable<Member[]>() {
                    public Iterator<Member[]> iterator() {
                        return new Iterator<Member[]>() {
                            int index = 0;
                            public boolean hasNext() {
                                return (index < list.size());
                            }
                            public Member[] next() {
                                return (Member[]) list.get(index++);
                            }
                            public void remove() {
                                throw new UnsupportedOperationException("remove");
                            }
                        };
                    }
                };
            }
        }
        protected List executeList() {
            SqlTupleReader tr = new SqlTupleReader(constraint);
            tr.setMaxRows(maxRows);
            for (CrossJoinArg arg : args) {
                addLevel(tr, arg);
            }

            // lookup the result in cache; we can't return the cached
            // result if the tuple reader contains a target with calculated
            // members because the cached result does not include those
            // members; so we still need to cross join the cached result
            // with those enumerated members
            Object key = tr.getCacheKey();
            List<List<RolapMember>> result = cache.get(key);
            boolean hasEnumTargets = (tr.getEnumTargetCount() > 0);
            if (result != null && !hasEnumTargets) {
                if (listener != null) {
                    TupleEvent e = new TupleEvent(this, tr);
                    listener.foundInCache(e);
                }
                return copy(result);
            }

            // execute sql and store the result
            if (result == null && listener != null) {
                TupleEvent e = new TupleEvent(this, tr);
                listener.excutingSql(e);
            }

            // if we don't have a cached result in the case where we have
            // enumerated targets, then retrieve and cache that partial result
            List<List<RolapMember>> partialResult = result;
            result = null;
            List<List<RolapMember>> newPartialResult = null;
            if (hasEnumTargets && partialResult == null) {
                newPartialResult = new ArrayList<List<RolapMember>>();
            }
            DataSource dataSource = schemaReader.getDataSource();
            if (args.length == 1) {
                result = (List) tr.readMembers(dataSource, partialResult, newPartialResult);
            } else {
                result = (List) tr.readTuples(dataSource, partialResult, newPartialResult);
            }

            if (hasEnumTargets) {
                if (newPartialResult != null) {
                    cache.put(key, newPartialResult);
                }
            } else {
                cache.put(key, result);
            }
            return copy(result);
        }

        /**
         * returns a copy of the result because its modified
         */
        private <T> List<T> copy(List<T> list) {
            return new ArrayList<T>(list);
        }

        private void addLevel(TupleReader tr, CrossJoinArg arg) {
            RolapLevel level = arg.getLevel();
            RolapHierarchy hierarchy = level.getHierarchy();
            MemberReader mr = hierarchy.getMemberReader(schemaReader.getRole());
            MemberBuilder mb = mr.getMemberBuilder();
            Util.assertTrue(mb != null, "MemberBuilder not found");

            if (arg instanceof MemberListCrossJoinArg &&
                ((MemberListCrossJoinArg) arg).hasCalcMembers())
            {
                // only need to keep track of the members in the case
                // where there are calculated members since in that case,
                // we produce the values by enumerating through the list
                // rather than generating the values through native sql
                tr.addLevelMembers(level, mb, arg.getMembers());
            } else {
                tr.addLevelMembers(level, mb, null);
            }
        }

        int getMaxRows() {
            return maxRows;
        }

        void setMaxRows(int maxRows) {
            this.maxRows = maxRows;
        }
    }

    /**
     * "Light version" of a {@link TupleConstraint}, represents one of
     * member.children, level.members, member.descendants, {enumeration}.
     *
     * @author av
     * @since Nov 14, 2005
     */
    protected interface CrossJoinArg {
        RolapLevel getLevel();

        RolapMember[] getMembers();

        void addConstraint(
            SqlQuery sqlQuery,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            Map<String, RolapStar.Table> relationNamesToStarTableMap);

        boolean isPreferInterpreter(boolean joinArg);
    }

    /**
     * represents one of
     * <ul>
     * <li>Level.Members:  member == null and level != null</li>
     * <li>Member.Children: member != null and level = member.getLevel().getChildLevel() </li>
     * <li>Member.Descendants: member != null and level == some level below member.getLevel()</li>
     * </ul>
     *
     * @author av
     * @since Nov 12, 2005
     */
    protected static class DescendantsCrossJoinArg implements CrossJoinArg {
        RolapMember member;
        RolapLevel level;

        public DescendantsCrossJoinArg(RolapLevel level, RolapMember member) {
            this.level = level;
            this.member = member;
        }

        public RolapLevel getLevel() {
            return level;
        }

        public RolapMember[] getMembers() {
            if (member == null) {
                return null;
            }
            return new RolapMember[] { member };
        }

        public boolean isPreferInterpreter(boolean joinArg) {
            return false;
        }

        private boolean equals(Object o1, Object o2) {
            return o1 == null ? o2 == null : o1.equals(o2);
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof DescendantsCrossJoinArg)) {
                return false;
            }
            DescendantsCrossJoinArg that = (DescendantsCrossJoinArg) obj;
            if (!equals(this.level, that.level)) {
                return false;
            }
            return equals(this.member, that.member);
        }

        public int hashCode() {
            int c = 1;
            if (level != null) {
                c = level.hashCode();
            }
            if (member != null) {
                c = 31 * c + member.hashCode();
            }
            return c;
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            Map<String, RolapStar.Table> relationNamesToStarTableMap) {
            if (member != null) {
                SqlConstraintUtils.addMemberConstraint(
                    sqlQuery, levelToColumnMap, relationNamesToStarTableMap,
                    null, member, true);
            }
        }
    }

    /**
     * Represents an enumeration {member1, member2, ...}.
     * All members must to the same level and are non-calculated.
     *
     * @author av
     * @since Nov 14, 2005
     */
    protected static class MemberListCrossJoinArg implements CrossJoinArg {
        private RolapMember[] members;
        private RolapLevel level = null;
        private boolean strict;
        private boolean hasCalcMembers;
        private boolean hasNonCalcMembers;

        private MemberListCrossJoinArg(
            RolapLevel level, RolapMember[] members, boolean strict,
            boolean hasCalcMembers, boolean hasNonCalcMembers) {
            this.level = level;
            this.members = members;
            this.strict = strict;
            this.hasCalcMembers = hasCalcMembers;
            this.hasNonCalcMembers = hasNonCalcMembers;
        }

        /**
         * Creates an instance of {@link RolapNativeSet.CrossJoinArg},
         * or returns null if the arguments are invalid.
         *
         * <p>To be valid, the arguments must be non-calculated members of the
         * same level (after filtering out any null members).  There must be at
         * least one member to begin with (may be null).  If all members are
         * nulls, then the result is a valid empty predicate.
         *
         * <p>REVIEW jvs 12-May-2007:  but according to the code, if
         * strict is false, then the argument is valid even if calculated
         * members are presented (and then it's flagged appropriately
         * for special handling downstream).
         */
        static CrossJoinArg create(Exp[] args, boolean strict) {
            if (args.length == 0) {
                return null;
            }
            RolapLevel level = null;
            RolapLevel nullLevel = null;
            boolean hasCalcMembers = false;
            boolean hasNonCalcMembers = false;
            int nNullMembers = 0;
            for (int i = 0; i < args.length; i++) {
                if (!(args[i] instanceof MemberExpr)) {
                    return null;
                }
                RolapMember m =
                    (RolapMember) ((MemberExpr) args[i]).getMember();

                if (m.isNull()) {
                    // we're going to filter out null members anyway;
                    // don't choke on the fact that their level
                    // doesn't match that of others
                    nullLevel = m.getLevel();
                    ++nNullMembers;
                    continue;
                }
                
                if (m.isCalculated()) {
                    if (strict) {
                        return null;
                    }
                    hasCalcMembers = true;
                } else {
                    hasNonCalcMembers = true;
                }
                if (level == null) {
                    level = m.getLevel();
                } else if (!level.equals(m.getLevel())) {
                    return null;
                }
            }
            if (level == null) {
                // all members were null; use an arbitrary one of the
                // null levels since the SQL predicate is going to always
                // fail anyway
                assert(nullLevel != null);
                level = nullLevel;
            }
            if (!isSimpleLevel(level)) {
                return null;
            }
            RolapMember[] members = new RolapMember[args.length - nNullMembers];
            
            int j = 0;
            for (int i = 0; i < args.length; ++i) {
                RolapMember m =
                    (RolapMember) ((MemberExpr) args[i]).getMember();
                if (m.isNull()) {
                    // filter out null members
                    continue;
                }
                members[j] = m;
                ++j;
            }
            
            return new MemberListCrossJoinArg(
                level, members, strict, hasCalcMembers, hasNonCalcMembers);
        }

        public RolapLevel getLevel() {
            return level;
        }

        public RolapMember[] getMembers() {
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

        public boolean hasCalcMembers() {
            return hasCalcMembers;
        }

        public int hashCode() {
            int c = 12;
            for (RolapMember member : members) {
                c = 31 * c + member.hashCode();
            }
            if (strict) {
                c += 1;
            }
            return c;
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof MemberListCrossJoinArg)) {
                return false;
            }
            MemberListCrossJoinArg that = (MemberListCrossJoinArg) obj;
            if (this.strict != that.strict) {
                return false;
            }
            for (int i = 0; i < members.length; i++) {
                if (this.members[i] != that.members[i]) {
                    return false;
                }
            }
            return true;
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            Map<RolapLevel, RolapStar.Column> levelToColumnMap,
            Map<String, RolapStar.Table> relationNamesToStarTableMap) {
            SqlConstraintUtils.addMemberConstraint(
                sqlQuery, levelToColumnMap, relationNamesToStarTableMap, null,
                Arrays.asList(members), strict, true);
        }
    }

    /**
     * Checks for Descendants(&lt;member&gt;, &lt;Level&gt;)
     *
     * @return an {@link CrossJoinArg} instance describing the Descendants
     *   function, or null if <code>fun</code> represents something else.
     */
    protected CrossJoinArg checkDescendants(FunDef fun, Exp[] args) {
        if (!"Descendants".equalsIgnoreCase(fun.getName())) {
            return null;
        }
        if (args.length != 2) {
            return null;
        }
        if (!(args[0] instanceof MemberExpr)) {
            return null;
        }
        RolapMember member = (RolapMember) ((MemberExpr) args[0]).getMember();
        if (member.isCalculated()) {
            return null;
        }
        if (!(args[1] instanceof LevelExpr)) {
            return null;
        }
        RolapLevel level = (RolapLevel) ((LevelExpr) args[1]).getLevel();
        if (!isSimpleLevel(level)) {
            return null;
        }
        return new DescendantsCrossJoinArg(level, member);
    }

    /**
     * Checks for <code>&lt;Level&gt;.Members</code>.
     *
     * @return an {@link CrossJoinArg} instance describing the Level.members
     *   function, or null if <code>fun</code> represents something else.
     */
    protected CrossJoinArg checkLevelMembers(FunDef fun, Exp[] args) {
        if (!"Members".equalsIgnoreCase(fun.getName())) {
            return null;
        }
        if (args.length != 1) {
            return null;
        }
        if (!(args[0] instanceof LevelExpr)) {
            return null;
        }
        RolapLevel level = (RolapLevel) ((LevelExpr) args[0]).getLevel();
        if (!isSimpleLevel(level)) {
            return null;
        }
        return new DescendantsCrossJoinArg(level, null);
    }

    /**
     * Checks for <code>&lt;Member&gt;.Children</code>.
     *
     * @return an {@link CrossJoinArg} instance describing the member.children
     *   function, or null if <code>fun</code> represents something else.
     */
    protected CrossJoinArg checkMemberChildren(FunDef fun, Exp[] args) {
        if (!"Children".equalsIgnoreCase(fun.getName())) {
            return null;
        }
        if (args.length != 1) {
            return null;
        }
        if (!(args[0] instanceof MemberExpr)) {
            return null;
        }
        RolapMember member = (RolapMember) ((MemberExpr) args[0]).getMember();
        if (member.isCalculated()) {
            return null;
        }
        RolapLevel level = member.getLevel();
        level = (RolapLevel) level.getChildLevel();
        if (level == null || !isSimpleLevel(level)) {
            // no child level
            return null;
        }
        return new DescendantsCrossJoinArg(level, member);
    }

    /**
     * Checks for a set constructor, <code>{member1, member2,
     * &#46;&#46;&#46;}</code>.
     *
     * @return an {@link CrossJoinArg} instance describing the enumeration,
     *    or null if <code>fun</code> represents something else.
     */
    protected CrossJoinArg checkEnumeration(FunDef fun, Exp[] args) {
        if (!"{}".equalsIgnoreCase(fun.getName())) {
            return null;
        }
        return MemberListCrossJoinArg.create(args, isStrict());
    }

    /**
     * Checks for <code>CrossJoin(&lt;set1&gt;, &lt;set2&gt)</code>, where
     * set1 and set2 are one of
     * <code>member.children</code>, <code>level.members</code> or
     * <code>member.descendants</code>.
     */
    protected CrossJoinArg[] checkCrossJoin(FunDef fun, Exp[] args) {
        // is this "CrossJoin([A].children, [B].children)"
        if (!"Crossjoin".equalsIgnoreCase(fun.getName()) &&
            !"NonEmptyCrossJoin".equalsIgnoreCase(fun.getName()))
        {
            return null;
        }
        if (args.length != 2) {
            return null;
        }
        CrossJoinArg[] arg0 = checkCrossJoinArg(args[0]);
        if (arg0 == null) {
            return null;
        }
        CrossJoinArg[] arg1 = checkCrossJoinArg(args[1]);
        if (arg1 == null) {
            return null;
        }
        CrossJoinArg[] ret = new CrossJoinArg[arg0.length + arg1.length];
        System.arraycopy(arg0, 0, ret, 0, arg0.length);
        System.arraycopy(arg1, 0, ret, arg0.length, arg1.length);
        return ret;
    }

    /**
     * Scans for memberChildren, levelMembers, memberDescendants, crossJoin.
     */
    protected CrossJoinArg[] checkCrossJoinArg(Exp exp) {
        if (exp instanceof NamedSetExpr) {
            NamedSet namedSet = ((NamedSetExpr) exp).getNamedSet();
            exp = namedSet.getExp();
        }
        if (!(exp instanceof ResolvedFunCall)) {
            return null;
        }
        final ResolvedFunCall funCall = (ResolvedFunCall) exp;
        FunDef fun = funCall.getFunDef();
        Exp[] args = funCall.getArgs();

        CrossJoinArg arg;
        arg = checkMemberChildren(fun, args);
        if (arg != null) {
            return new CrossJoinArg[] {arg};
        }
        arg = checkLevelMembers(fun, args);
        if (arg != null) {
            return new CrossJoinArg[] {arg};
        }
        arg = checkDescendants(fun, args);
        if (arg != null) {
            return new CrossJoinArg[] {arg};
        }
        arg = checkEnumeration(fun, args);
        if (arg != null) {
            return new CrossJoinArg[] {arg};
        }
        return checkCrossJoin(fun, args);
    }

    /**
     * Ensures that level is not ragged and not a parent/child level.
     */
    protected static boolean isSimpleLevel(RolapLevel level) {
        RolapHierarchy hier = level.getHierarchy();
        // does not work with ragged hierarchies
        if (hier.isRagged()) {
            return false;
        }
        // does not work with parent/child
        if (level.isParentChild()) {
            return false;
        }
        // does not work for measures
        if (level.isMeasure()) {
            return false;
        }
        return true;
    }

    /**
     * Tests whether non-native evaluation is preferred for the
     * given arguments.
     *
     * @param joinArg true if evaluating a cross-join; false if
     * evaluating a single-input expression such as filter
     *
     * @return true if <em>all</em> args prefer the interpreter
     */
    protected boolean isPreferInterpreter(
        CrossJoinArg[] args, boolean joinArg) {
        for (CrossJoinArg arg : args) {
            if (!arg.isPreferInterpreter(joinArg)) {
                return false;
            }
        }
        return true;
    }

    /** disable garbage collection for test */
    void useHardCache(boolean hard) {
        if (hard) {
            cache = new HardSmartCache();
        } else {
            cache = new SoftSmartCache();
        }
    }

    /**
     * Override current members in position by default members in
     * hierarchies which are involved in this filter/topcount.
     * Stores the RolapStoredMeasure into the context because that is needed to
     * generate a cell request to constraint the sql.
     *
     * The current context may contain a calculated measure, this measure
     * was translated into an sql condition (filter/topcount). The measure
     * is not used to constrain the result but only to access the star.
     *
     * @see RolapAggregationManager#makeRequest(Member[], boolean, boolean)
     */
    protected RolapEvaluator overrideContext(
        RolapEvaluator evaluator,
        CrossJoinArg[] cargs,
        RolapStoredMeasure storedMeasure)
    {
        SchemaReader schemaReader = evaluator.getSchemaReader();
        RolapEvaluator newEvaluator = (RolapEvaluator) evaluator.push();
        for (CrossJoinArg carg : cargs) {
            Hierarchy hierarchy = carg.getLevel().getHierarchy();
            Member defaultMember =
                schemaReader.getHierarchyDefaultMember(hierarchy);
            newEvaluator.setContext(defaultMember);
        }
        if (storedMeasure != null)
            newEvaluator.setContext(storedMeasure);
        return newEvaluator;
    }
}

// End RolapNativeSet.java
