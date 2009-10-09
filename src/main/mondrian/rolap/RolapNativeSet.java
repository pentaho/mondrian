/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import java.util.*;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.cache.HardSmartCache;
import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.MemberChildrenConstraint;
import mondrian.rolap.sql.SqlQuery;
import mondrian.rolap.sql.TupleConstraint;
import mondrian.mdx.*;

import static mondrian.rolap.RolapLevel.HideMemberCondition.*;

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
    protected static final Logger LOGGER =
        Logger.getLogger(RolapNativeSet.class);

    private SmartCache<Object, List<List<RolapMember>>> cache =
        new SoftSmartCache<Object, List<List<RolapMember>>>();

    /**
     * Returns whether certain member types(e.g. calculated members) should
     * disable native SQL evaluation for expressions containing them.
     *
     * <p>
     * If true, expressions containing calculated members will be evaluated by
     * the interpreter, instead of using SQL.
     *
     * If false, calc members will be ignored and the computation will be done
     * in SQL, returning more members than requested.  This is ok, if
     * the superflous members are filtered out in java code afterwards.
     * </p>
     */
    protected abstract boolean restrictMemberTypes();

    /**
     * Constraint for non empty {crossjoin, member.children,
     * member.descendants, level.members}
     */
    protected static abstract class SetConstraint extends SqlContextConstraint {
        CrossJoinArg[] args;

        SetConstraint(
            CrossJoinArg[] args,
            RolapEvaluator evaluator,
            boolean strict)
        {
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
            RolapCube baseCube,
            AggStar aggStar)
        {
            super.addConstraint(sqlQuery, baseCube, aggStar);
            for (CrossJoinArg arg : args) {
                // if the cross join argument has calculated members in its
                // enumerated set, ignore the constraint since we won't
                // produce that set through the native sql and instead
                // will simply enumerate through the members in the set
                if (!(arg instanceof MemberListCrossJoinArg)
                    || !((MemberListCrossJoinArg) arg).hasCalcMembers())
                {
                    arg.addConstraint(sqlQuery, baseCube, aggStar);
                }
            }
        }

        /**
         * Returns null to prevent the member/childern from being cached. There
         * exists no valid MemberChildrenConstraint that would fetch those
         * children that were extracted as a side effect from evaluating a non
         * empty crossjoin
         */
        public MemberChildrenConstraint getMemberChildrenConstraint(
            RolapMember parent)
        {
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
                if (!(arg instanceof MemberListCrossJoinArg)
                    || !((MemberListCrossJoinArg) arg).hasCalcMembers())
                {
                    key.add(arg);
                }
            }
            return key;
        }
    }

    protected class SetEvaluator implements NativeEvaluator {
        private final CrossJoinArg[] args;
        private final SchemaReaderWithMemberReaderAvailable schemaReader;
        private final TupleConstraint constraint;
        private int maxRows = 0;

        public SetEvaluator(
            CrossJoinArg[] args,
            SchemaReader schemaReader,
            TupleConstraint constraint)
        {
            this.args = args;
            if (schemaReader instanceof SchemaReaderWithMemberReaderAvailable) {
                this.schemaReader =
                    (SchemaReaderWithMemberReaderAvailable) schemaReader;
            } else {
                this.schemaReader =
                    new SchemaReaderWithMemberReaderCache(schemaReader);
            }
            this.constraint = constraint;
        }

        public Object execute(ResultStyle desiredResultStyle) {
            switch (desiredResultStyle) {
            case ITERABLE:
                return executeList(new HighCardSqlTupleReader(constraint));
            case MUTABLE_LIST:
            case LIST:
                return executeList(new SqlTupleReader(constraint));
            }
            throw ResultStyleException.generate(
                ResultStyle.ITERABLE_MUTABLELIST_LIST,
                Collections.singletonList(desiredResultStyle));
        }

        protected List executeList(final SqlTupleReader tr) {
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
                result = (List) tr.readMembers(
                    dataSource, partialResult, newPartialResult);
            } else {
                result = (List) tr.readTuples(
                    dataSource, partialResult, newPartialResult);
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
//            return new ArrayList<T>(list);
            return list;
        }

        private void addLevel(TupleReader tr, CrossJoinArg arg) {
            RolapLevel level = arg.getLevel();
            if (level == null) {
                // Level can be null if the CrossJoinArg represent
                // an empty set.
                // This is used to push down the "1 = 0" predicate
                // into the emerging CJ so that the entire CJ can
                // be natively evaluated.
                return;
            }

            RolapHierarchy hierarchy = level.getHierarchy();
            MemberReader mr = schemaReader.getMemberReader(hierarchy);
            MemberBuilder mb = mr.getMemberBuilder();
            Util.assertTrue(mb != null, "MemberBuilder not found");

            if (arg instanceof MemberListCrossJoinArg
                && ((MemberListCrossJoinArg) arg).hasCalcMembers())
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
     */
    protected interface CrossJoinArg {
        RolapLevel getLevel();

        List<RolapMember> getMembers();

        void addConstraint(
                SqlQuery sqlQuery,
                RolapCube baseCube,
                AggStar aggStar);

        boolean isPreferInterpreter(boolean joinArg);
    }

    /**
     * Represents one of:
     * <ul>
     * <li>Level.Members:  member == null and level != null</li>
     * <li>Member.Children: member != null and level =
     *     member.getLevel().getChildLevel()</li>
     * <li>Member.Descendants: member != null and level == some level below
     *     member.getLevel()</li>
     * </ul>
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

        public List<RolapMember> getMembers() {
            if (member == null) {
                return null;
            }
            final List<RolapMember> list = new ArrayList<RolapMember>();
            list.add(member);
            return list;
        }

        public void addConstraint(
            SqlQuery sqlQuery,
            RolapCube baseCube,
            AggStar aggStar)
        {
            if (member != null) {
                SqlConstraintUtils.addMemberConstraint(
                    sqlQuery, baseCube, aggStar, member, true);
            }
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
    }

    /**
     * Represents an enumeration {member1, member2, ...}.
     * All members must to the same level and are non-calculated.
     */
    protected static class MemberListCrossJoinArg implements CrossJoinArg {
        private List<RolapMember> members;
        private RolapLevel level = null;
        private boolean restrictMemberTypes;
        private boolean hasCalcMembers;
        private boolean hasNonCalcMembers;
        private boolean hasAllMember;

        private MemberListCrossJoinArg(
            RolapLevel level,
            List<RolapMember> members,
            boolean restrictMemberTypes,
            boolean hasCalcMembers,
            boolean hasNonCalcMembers,
            boolean hasAllMember)
        {
            this.level = level;
            this.members = members;
            this.restrictMemberTypes = restrictMemberTypes;
            this.hasCalcMembers = hasCalcMembers;
            this.hasNonCalcMembers = hasNonCalcMembers;
            this.hasAllMember = hasAllMember;
        }

        /**
         * Creates an instance of {@link RolapNativeSet.CrossJoinArg},
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
         * @return MemberListCrossJoinArg if member list is well formed,
         * NULL if not.
         */
        static CrossJoinArg create(
            RolapEvaluator evaluator,
            final List<RolapMember> args,
            final boolean restrictMemberTypes)
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
            int nNullMembers = 0;
            try {
                for (RolapMember m : args) {
                    if (m.isNull()) {
                        // we're going to filter out null members anyway;
                        // don't choke on the fact that their level
                        // doesn't match that of others
                        nullLevel = m.getLevel();
                        ++nNullMembers;
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
            } catch (ClassCastException cce) {
                return null;
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
            if ((level != null) && (!isSimpleLevel(level)
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
                hasCalcMembers, hasNonCalcMembers, hasAllMember);
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
                members, restrictMemberTypes, true);
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
            for (int i = 0; i < members.size(); i++) {
                if (this.members.get(i) != that.members.get(i)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Checks for Descendants(&lt;member&gt;, &lt;Level&gt;)
     *
     * @return an {@link CrossJoinArg} instance describing the Descendants
     *   function, or null if <code>fun</code> represents something else.
     */
    protected CrossJoinArg checkDescendants(
        Role role,
        FunDef fun,
        Exp[] args)
    {
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
        RolapLevel level = null;
        if ((args[1] instanceof LevelExpr)) {
            level = (RolapLevel) ((LevelExpr) args[1]).getLevel();
        } else if (args[1] instanceof Literal) {
            RolapLevel[] levels = (RolapLevel[])
                    member.getHierarchy().getLevels();
            int currentDepth = member.getDepth();
            Literal descendantsDepth = (Literal)args[1];
            int newDepth = currentDepth + descendantsDepth.getIntValue();
            if (newDepth < levels.length) {
                level = levels[newDepth];
            }
        } else {
            return null;
        }

        if (!isSimpleLevel(level)) {
            return null;
        }
        // Descendants of a member in an access-controlled hierarchy cannot be
        // converted to SQL. (We could be smarter; we don't currently notice
        // when the member is in a part of the hierarchy that is not
        // access-controlled.)
        final Access access = role.getAccess(level.getHierarchy());
        switch (access) {
        case ALL:
            break;
        default:
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
    protected CrossJoinArg checkLevelMembers(
        Role role,
        FunDef fun,
        Exp[] args)
    {
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
        // Members of a level in an access-controlled hierarchy cannot be
        // converted to SQL. (We could be smarter; we don't currently notice
        // when the level is in a part of the hierarchy that is not
        // access-controlled.)
        final Access access = role.getAccess(level.getHierarchy());
        switch (access) {
        case ALL:
            break;
        default:
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
    protected CrossJoinArg checkMemberChildren(
        Role role,
        FunDef fun,
        Exp[] args)
    {
        if (!"Children".equalsIgnoreCase(fun.getName())) {
            return null;
        }
        if (args.length != 1) {
            return null;
        }

        // Note: <Dimension>.Children is not recognized as a native expression.
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
        // Children of a member in an access-controlled hierarchy cannot be
        // converted to SQL. (We could be smarter; we don't currently notice
        // when the member is in a part of the hierarchy that is not
        // access-controlled.)
        final Access access = role.getAccess(level.getHierarchy());
        switch (access) {
        case ALL:
            break;
        default:
            return null;
        }
        return new DescendantsCrossJoinArg(level, member);
    }

    private static boolean isArgSizeSupported(
        RolapEvaluator evaluator,
        int argSize)
    {
        boolean argSizeNotSupported = false;

        // Note: srg size 0 is accepted as valid CJ argument
        // This is used to push down the "1 = 0" predicate
        // into the emerging CJ so that the entire CJ can
        // be natively evaluated.

        // First check that the member list will not result in a predicate
        // longer than the underlying DB could support.
        if (!evaluator.getDialect().supportsUnlimitedValueList()
            && argSize > MondrianProperties.instance().MaxConstraints.get())
        {
            argSizeNotSupported = true;
        }

        return (!argSizeNotSupported);
    }

    /**
     * Checks for a set constructor, <code>{member1, member2,
     * &#46;&#46;&#46;}</code> that does not contain calculated members.
     *
     * @return an {@link CrossJoinArg} instance describing the enumeration,
     *    or null if <code>fun</code> represents something else.
     */
    protected CrossJoinArg checkEnumeration(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args)
    {
        // Return null if not the expected funciton name or input size.
        if (!"{}".equalsIgnoreCase(fun.getName())
            || !isArgSizeSupported(evaluator, args.length))
        {
            return null;
        }

        List<RolapMember> memberList = new ArrayList<RolapMember>();
        for (int i = 0; i < args.length; ++i) {
            if (!(args[i] instanceof MemberExpr)
                || (((MemberExpr) args[i]).getMember().isCalculated()
                && !((MemberExpr) args[i]).getMember().isParentChildLeaf()))
            {
                // also returns null if any member is calculated
                return null;
            }
            memberList.add(
                (RolapMember) (((MemberExpr)args[i]).getMember()));
        }

        return MemberListCrossJoinArg.create(
            evaluator, memberList, restrictMemberTypes());
    }

    /**
     * Checks for <code>CrossJoin(&lt;set1&gt;, &lt;set2&gt;)</code>, where
     * set1 and set2 are one of
     * <code>member.children</code>, <code>level.members</code> or
     * <code>member.descendants</code>.
     *
     * @param evaluator Evaluator to use if inputs are to be evaluated
     * @param fun The function, either "CrossJoin" or "NonEmptyCrossJoin"
     * @param args Inputs to the CrossJoin
     * @param returnAny indicates we should return any valid crossjoin args
     * @return array of CrossJoinArg representing the inputs
     */
    protected CrossJoinArg[] checkCrossJoin(
        RolapEvaluator evaluator,
        FunDef fun,
        Exp[] args, final boolean returnAny)
    {
        // is this "CrossJoin([A].children, [B].children)"
        if (!"Crossjoin".equalsIgnoreCase(fun.getName())
            && !"NonEmptyCrossJoin".equalsIgnoreCase(fun.getName()))
        {
            return null;
        }
        if (args.length != 2) {
            return null;
        }
        // Check if the arguments can be natively evaluated.
        // If not, try evaluating this argument and turning the result into
        // MemberListCrossJoinArg.
        CrossJoinArg[][] argArray = new CrossJoinArg[2][];
        for (int i = 0; i < 2; i++) {
            argArray[i] = checkCrossJoinArg(evaluator, args[i], returnAny);
            if (argArray[i] == null && !returnAny) {
                argArray[i] = expandNonNative(evaluator, args[i]);
            }
            if (argArray[i] == null && !returnAny) {
                return null;
            }
        }
        CrossJoinArg[] ret =
            new CrossJoinArg[length(argArray, 0) + length(argArray, 1)];
        if (argArray[0] != null) {
            System.arraycopy(argArray[0], 0, ret, 0, length(argArray, 0));
        }
        if (argArray[1] != null) {
        System.arraycopy(
            argArray[1], 0, ret, length(argArray, 0), length(argArray, 1));
        }
        return ret;
    }
    private int length(final CrossJoinArg[][] argArray, final int index) {
        if (argArray[index] != null) {
            return argArray[index].length;
        }
        return 0;
    }

    private CrossJoinArg[] expandNonNative(
        RolapEvaluator evaluator,
        Exp exp)
    {
        ExpCompiler compiler = evaluator.getQuery().createCompiler();
        CrossJoinArg[] arg0 = null;
        if (MondrianProperties.instance().ExpandNonNative.get()) {
                ListCalc listCalc0 = compiler.compileList(exp);
                List<RolapMember> list0 =
                    Util.cast(listCalc0.evaluateList(evaluator));
                // Prevent the case when the second argument size is too large
                if (list0 != null) {
                    Util.checkCJResultLimit(list0.size());
                }
                CrossJoinArg arg =
                    MemberListCrossJoinArg.create(
                        evaluator, list0, restrictMemberTypes());
                if (arg != null) {
                    arg0 = new CrossJoinArg[] {arg};
                }
            }
            return arg0;
    }
    /**
     * Scans for memberChildren, levelMembers, memberDescendants, crossJoin.
     */
    protected CrossJoinArg[] checkCrossJoinArg(
        RolapEvaluator evaluator,
        Exp exp)
    {
        return checkCrossJoinArg(evaluator,exp,false);
    }

    protected CrossJoinArg[] checkCrossJoinArg(
        RolapEvaluator evaluator,
        Exp exp, final boolean returnAny)
    {
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


        final Role role = evaluator.getSchemaReader().getRole();
        CrossJoinArg arg;

        arg = checkMemberChildren(role, fun, args);
        if (arg != null) {
            return new CrossJoinArg[] {arg};
        }
        arg = checkLevelMembers(role, fun, args);
        if (arg != null) {
            return new CrossJoinArg[] {arg};
        }
        arg = checkDescendants(role, fun, args);
        if (arg != null) {
            return new CrossJoinArg[] {arg};
        }
        arg = checkEnumeration(evaluator, fun, args);
        if (arg != null) {
            return new CrossJoinArg[] {arg};
        }
        // strip off redundant set braces, for example
        // { Gender.Gender.members }, or {{{ Gender.M }}}
        if ("{}".equalsIgnoreCase(fun.getName()) && args.length == 1) {
            return checkCrossJoinArg(evaluator, args[0], returnAny);
        }
        if ("NativizeSet".equalsIgnoreCase(fun.getName()) && args.length == 1) {
            return checkCrossJoinArg(evaluator, args[0], returnAny);
        }
        return checkCrossJoin(evaluator, fun, args,returnAny);
    }

    /**
     * Ensures that level is not ragged and not a parent/child level.
     */
    protected static boolean isSimpleLevel(RolapLevel level) {
        // does not work with ragged hierarchies except in the
        // simplest cases -- see isToRagged.
        if (isTooRagged(level)) {
            return false;
        }
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
     * Determines whether the specified level is too ragged for native
     * evaluation, which is able to handle one special case of a ragged
     * hierarchy: when the level specified in the query is the leaf level of
     * the hierarchy and HideMemberCondition for the level is IfBlankName.
     * This is true even if higher levels of the hierarchy can be hidden
     * because even in that case the only column that needs to be read is the
     * column that holds the leaf. IfParentsName can't be handled even at the
     * leaf level because in the general case we aren't reading the column
     * that holds the parent. Also, IfBlankName can't be handled for non-leaf
     * levels because we would have to read the column for the next level
     * down for members with blank names.
     *
     * @param level A RolapLevel to check the raggedness of.
     * @return true if the specified level is too ragged for native
     *         evaluation.
     */
    protected static boolean isTooRagged(
        RolapLevel level)
    {
        // Is this the special case of raggedness that native evaluation
        // is able to handle?
        if (level.getDepth() == level.getHierarchy().getLevels().length - 1) {
            return (level.getHideMemberCondition() != Never
                && level.getHideMemberCondition() != IfBlankName);
        }
        // Handle the general case in the traditional way.
        return level.getHierarchy().isRagged();
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
        CrossJoinArg[] args,
        boolean joinArg)
    {
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
     * @see RolapAggregationManager#makeRequest(RolapEvaluator)
     */
    protected RolapEvaluator overrideContext(
        RolapEvaluator evaluator,
        CrossJoinArg[] cargs,
        RolapStoredMeasure storedMeasure)
    {
        SchemaReader schemaReader = evaluator.getSchemaReader();
        RolapEvaluator newEvaluator = (RolapEvaluator) evaluator.push();
        for (CrossJoinArg carg : cargs) {
            RolapLevel level = carg.getLevel();
            if (level != null) {
                Hierarchy hierarchy = level.getHierarchy();
                Member defaultMember =
                    schemaReader.getHierarchyDefaultMember(hierarchy);
                newEvaluator.setContext(defaultMember);
            }
        }
        if (storedMeasure != null) {
            newEvaluator.setContext(storedMeasure);
        }
        return newEvaluator;
    }


    public interface SchemaReaderWithMemberReaderAvailable
        extends SchemaReader
    {
        MemberReader getMemberReader(Hierarchy hierarchy);
    }

    private static class SchemaReaderWithMemberReaderCache
        extends DelegatingSchemaReader
        implements SchemaReaderWithMemberReaderAvailable
    {
        private final Map<Hierarchy, MemberReader> hierarchyReaders =
            new HashMap<Hierarchy, MemberReader>();

        SchemaReaderWithMemberReaderCache(SchemaReader schemaReader) {
            super(schemaReader);
        }

        public synchronized MemberReader getMemberReader(Hierarchy hierarchy) {
            MemberReader memberReader = hierarchyReaders.get(hierarchy);
            if (memberReader == null) {
                memberReader =
                    ((RolapHierarchy) hierarchy).createMemberReader(
                        schemaReader.getRole());
                hierarchyReaders.put(hierarchy, memberReader);
            }
            return memberReader;
        }
    }
}

// End RolapNativeSet.java
