/*
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import mondrian.olap.*;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.cache.HardSmartCache;
import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.mdx.MemberExpr;
import mondrian.mdx.LevelExpr;

import org.apache.log4j.Logger;

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
    private SmartCache cache = new SoftSmartCache();

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

        public void addConstraint(SqlQuery sqlQuery) {
            super.addConstraint(sqlQuery);
            for (int i = 0; i < args.length; i++) {
                CrossJoinArg arg = args[i];
                arg.addConstraint(sqlQuery);
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
            List key = new ArrayList();
            key.add(super.getCacheKey());
            key.addAll(Arrays.asList(args));
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

        public Object execute() {
            SqlTupleReader tr = new SqlTupleReader(constraint);
            tr.setMaxRows(maxRows);
            for (int i = 0; i < args.length; i++) {
                addLevel(tr, args[i]);
            }

            // lookup the result in cache
            Object key = tr.getCacheKey();
            List result = (List) cache.get(key);
            if (result != null) {
                if (listener != null) {
                    TupleEvent e = new TupleEvent(this, tr);
                    listener.foundInCache(e);
                }
                return copy(result);
            }

            // execute sql and store the result
            if (listener != null) {
                TupleEvent e = new TupleEvent(this, tr);
                listener.excutingSql(e);
            }
            result = tr.readTuples(schemaReader.getDataSource());
            cache.put(key, result);
            return copy(result);
        }


        /**
         * returns a copy of the result because its modified
         */
        private List copy(List list) {
            List copy = new ArrayList();
            copy.addAll(list);
            return copy;
        }

        private void addLevel(TupleReader tr, CrossJoinArg arg) {
            RolapLevel level = arg.getLevel();
            RolapHierarchy hierarchy = (RolapHierarchy) level.getHierarchy();
            MemberReader mr = hierarchy.getMemberReader(schemaReader.getRole());
            MemberBuilder mb = mr.getMemberBuilder();
            Util.assertTrue(mb != null, "MemberBuilder not found");
            tr.addLevelMembers(level, mb);
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

        void addConstraint(SqlQuery sqlQuery);

        boolean isPreferInterpreter();
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

        public boolean isPreferInterpreter() {
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
            if (level != null)
                c = level.hashCode();
            if (member != null)
                c = 31 * c + member.hashCode();
            return c;
        }

        public void addConstraint(SqlQuery sqlQuery) {
            if (member != null)
                SqlConstraintUtils.addMemberConstraint(sqlQuery, member, true);
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

        private MemberListCrossJoinArg(RolapLevel level, RolapMember[] members, boolean strict) {
            this.level = level;
            this.members = members;
            this.strict = strict;
        }

        /**
         * Creates an instance of CrossJoinArg, or returns null if the
         * arguments are invalid.<p/>
         *
         * To be valid, the arguments must be non-calculated members of the
         * same level.
         */
        static CrossJoinArg create(Exp[] args, boolean strict) {
            if (args.length == 0) {
                return null;
            }
            RolapLevel level = null;
            for (int i = 0; i < args.length; i++) {
                if (!(args[i] instanceof MemberExpr)) {
                    return null;
                }
                RolapMember m = (RolapMember) ((MemberExpr) args[i]).getMember();
                if (strict && m.isCalculated()) {
                    return null;
                }
                if (i == 0) {
                    level = m.getRolapLevel();
                } else if (!level.equals(m.getLevel())) {
                    return null;
                }
            }
            if (!isSimpleLevel(level)) {
                return null;
            }
            RolapMember[] members = new RolapMember[args.length];
            for (int i = 0; i < members.length; i++) {
                members[i] = (RolapMember) ((MemberExpr) args[i]).getMember();
            }
            return new MemberListCrossJoinArg(level, members, strict);
        }

        public RolapLevel getLevel() {
            return level;
        }

        public boolean isPreferInterpreter() {
            return true;
        }

        public int hashCode() {
            int c = 12;
            for (int i = 0; i < members.length; i++)
                c = 31 * c + members[i].hashCode();
            if (strict)
                c += 1;
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

        public void addConstraint(SqlQuery sqlQuery) {
            SqlConstraintUtils.addMemberConstraint(sqlQuery, Arrays.asList(members), strict);
        }

    }

    /**
     * Checks for Descendants(&lt;member&gt;, &lt;Level&gt;)
     *
     * @return an {@link CrossJoinArg} instance describing the Descendants function or null,
     * if <code>fun</code> represents something else.
     */
    protected CrossJoinArg checkDescendants(FunDef fun, Exp[] args) {
        if (!"Descendants".equalsIgnoreCase(fun.getName()))
            return null;
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
        RolapLevel level = member.getRolapLevel();
        level = (RolapLevel) level.getChildLevel();
        if (level == null || !isSimpleLevel(level)) {
            // no child level
            return null;
        }
        return new DescendantsCrossJoinArg(level, member);
    }

    /**
     * Checks for a set constructor, <code>{member1, member2, ...}</code>.
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
        if (!"Crossjoin".equalsIgnoreCase(fun.getName()))
            return null;
        if (args.length != 2)
            return null;
        CrossJoinArg[] arg0 = checkCrossJoinArg(args[0]);
        if (arg0 == null)
            return null;
        CrossJoinArg[] arg1 = checkCrossJoinArg(args[1]);
        if (arg1 == null)
            return null;
        CrossJoinArg[] ret = new CrossJoinArg[arg0.length + arg1.length];
        System.arraycopy(arg0, 0, ret, 0, arg0.length);
        System.arraycopy(arg1, 0, ret, arg0.length, arg1.length);
        return ret;
    }

    /**
     * Scans for memberChildren, levelMembers, memberDescendants, crossJoin.
     */
    protected CrossJoinArg[] checkCrossJoinArg(Exp exp) {
        if (!(exp instanceof FunCall)) {
            return null;
        }
        FunDef fun = ((FunCall) exp).getFunDef();
        Exp[] args = ((FunCall) exp).getArgs();
        return checkCrossJoinArg(fun, args);
    }

    /**
     * Scans for memberChildren, levelMembers, memberDescendants, crossJoin (recursive)
     */
    protected CrossJoinArg[] checkCrossJoinArg(FunDef fun, Exp[] args) {
        CrossJoinArg arg = checkMemberChildren(fun, args);
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
        RolapHierarchy hier = (RolapHierarchy) level.getHierarchy();
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
     * If all involved sets are already known, like in crossjoin({a,b}, {c,d}),
     * then use the interpreter.
     *
     * @return true if <em>all</em> args are prefer the interpreter
     */
    protected boolean isPreferInterpreter(CrossJoinArg[] args) {
        for (int i = 0; i < args.length; i++) {
            if (!args[i].isPreferInterpreter()) {
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
}

// End RolapNativeSet.java
