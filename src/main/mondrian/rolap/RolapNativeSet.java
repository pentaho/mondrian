/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2009 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.olap.*;
import mondrian.rolap.TupleReader.MemberBuilder;
import mondrian.rolap.aggmatcher.AggStar;
import mondrian.rolap.cache.HardSmartCache;
import mondrian.rolap.cache.SmartCache;
import mondrian.rolap.cache.SoftSmartCache;
import mondrian.rolap.sql.*;

import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.util.*;

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

    protected CrossJoinArgFactory crossJoinArgFactory() {
        return new CrossJoinArgFactory(restrictMemberTypes());
    }

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
         * {@inheritDoc}
         *
         * <p>If there is a crossjoin, we need to join the fact table - even if
         * the evaluator context is empty.
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
                    RolapLevel level = arg.getLevel();
                    if (level == null || levelIsOnBaseCube(baseCube, level)) {
                        arg.addConstraint(sqlQuery, baseCube, aggStar);
                    }
                }
            }
        }

        private boolean levelIsOnBaseCube(
            final RolapCube baseCube, final RolapLevel level)
        {
            return baseCube.findBaseCubeHierarchy(level.getHierarchy()) != null;
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
                listener.executingSql(e);
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
        RolapEvaluator newEvaluator = evaluator.push();
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

