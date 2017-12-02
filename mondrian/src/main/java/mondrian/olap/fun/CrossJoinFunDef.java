/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2002-2005 Julian Hyde
// Copyright (C) 2005-2017 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.mdx.*;
import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.resource.MondrianResource;
import mondrian.rolap.RolapEvaluator;
import mondrian.rolap.SqlConstraintUtils;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.util.CancellationChecker;
import mondrian.util.CartesianProductList;

import org.apache.log4j.Logger;

import java.util.*;


/**
 * Definition of the <code>CrossJoin</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
public class CrossJoinFunDef extends FunDefBase {
    private static final Logger LOGGER =
        Logger.getLogger(CrossJoinFunDef.class);

    static final ReflectiveMultiResolver Resolver =
        new ReflectiveMultiResolver(
            "Crossjoin",
            "Crossjoin(<Set1>, <Set2>)",
            "Returns the cross product of two sets.",
            new String[]{"fxxx"},
            CrossJoinFunDef.class);

    static final StarCrossJoinResolver StarResolver =
        new StarCrossJoinResolver();

    private static int counterTag = 0;

    // used to tell the difference between crossjoin expressions.
    private final int ctag = counterTag++;

    public CrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        // CROSSJOIN(<Set1>,<Set2>) has type [Hie1] x [Hie2].
        List<MemberType> list = new ArrayList<MemberType>();
        for (Exp arg : args) {
            final Type type = arg.getType();
            if (type instanceof SetType) {
                addTypes(type, list);
            } else if (getName().equals("*")) {
                // The "*" form of CrossJoin is lenient: args can be either
                // members/tuples or sets.
                addTypes(type, list);
            } else {
                throw Util.newInternal("arg to crossjoin must be a set");
            }
        }
        final MemberType[] types = list.toArray(new MemberType[list.size()]);
        TupleType.checkHierarchies(types);
        final TupleType tupleType = new TupleType(types);
        return new SetType(tupleType);
    }

    /**
     * Adds a type to a list of types. If type is a {@link TupleType}, does so
     * recursively.
     *
     * @param type Type to add to list
     * @param list List of types to add to
     */
    private static void addTypes(final Type type, List<MemberType> list) {
        if (type instanceof SetType) {
            SetType setType = (SetType) type;
            addTypes(setType.getElementType(), list);
        } else if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            for (Type elementType : tupleType.elementTypes) {
                addTypes(elementType, list);
            }
        } else if (type instanceof MemberType) {
            list.add((MemberType) type);
        } else {
            throw Util.newInternal("Unexpected type: " + type);
        }
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        // What is the desired return type?
        for (ResultStyle r : compiler.getAcceptableResultStyles()) {
            switch (r) {
            case ITERABLE:
            case ANY:
                // Consumer wants ITERABLE or ANY
                    return compileCallIterable(call, compiler);
            case LIST:
                // Consumer wants (immutable) LIST
                return compileCallImmutableList(call, compiler);
            case MUTABLE_LIST:
                // Consumer MUTABLE_LIST
                return compileCallMutableList(call, compiler);
            }
        }
        throw ResultStyleException.generate(
            ResultStyle.ITERABLE_LIST_MUTABLELIST_ANY,
            compiler.getAcceptableResultStyles());
    }

    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////
    // Iterable
    ///////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////

    protected IterCalc compileCallIterable(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        final Calc calc1 = toIter(compiler, call.getArg(0));
        final Calc calc2 = toIter(compiler, call.getArg(1));
        Calc[] calcs = new Calc[] {calc1, calc2};
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: ITERABLE, LIST or MUTABLE_LIST, but
        // LIST and MUTABLE_LIST are treated the same; so
        // there are 16 possible combinations - sweet.

        // Check returned calc ResultStyles
        checkIterListResultStyles(calc1);
        checkIterListResultStyles(calc2);

        return new CrossJoinIterCalc(call, calcs);
    }

    private Calc toIter(ExpCompiler compiler, final Exp exp) {
        // Want iterable, immutable list or mutable list in that order
        // It is assumed that an immutable list is easier to get than
        // a mutable list.
        final Type type = exp.getType();
        if (type instanceof SetType) {
            // this can return an IterCalc or ListCalc
            return compiler.compileAs(
                exp,
                null,
                ResultStyle.ITERABLE_LIST_MUTABLELIST);
        } else {
            // this always returns an IterCalc
            return new SetFunDef.ExprIterCalc(
                new DummyExp(new SetType(type)),
                new Exp[] {exp},
                compiler,
                ResultStyle.ITERABLE_LIST_MUTABLELIST);
        }
    }

    class CrossJoinIterCalc extends AbstractIterCalc
    {
        CrossJoinIterCalc(ResolvedFunCall call, Calc[] calcs) {
            super(call, calcs);
        }

        public TupleIterable evaluateIterable(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                schemaReader.getNativeSetEvaluator(
                    call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (TupleIterable)
                    nativeEvaluator.execute(ResultStyle.ITERABLE);
            }

            Calc[] calcs = getCalcs();
            IterCalc calc1 = (IterCalc) calcs[0];
            IterCalc calc2 = (IterCalc) calcs[1];

            TupleIterable o1 = calc1.evaluateIterable(evaluator);
            if (o1 instanceof TupleList) {
                TupleList l1 = (TupleList) o1;
                l1 = nonEmptyOptimizeList(evaluator, l1, call);
                if (l1.isEmpty()) {
                    return TupleCollections.emptyList(getType().getArity());
                }
                o1 = l1;
            }

            TupleIterable o2 = calc2.evaluateIterable(evaluator);
            if (o2 instanceof TupleList) {
                TupleList l2 = (TupleList) o2;
                l2 = nonEmptyOptimizeList(evaluator, l2, call);
                if (l2.isEmpty()) {
                    return TupleCollections.emptyList(getType().getArity());
                }
                o2 = l2;
            }

            return makeIterable(o1, o2);
        }

        protected TupleIterable makeIterable(
            final TupleIterable it1,
            final TupleIterable it2)
        {
            // There is no knowledge about how large either it1 ore it2
            // are or how many null members they might have, so all
            // one can do is iterate across them:
            // iterate across it1 and for each member iterate across it2

            return new AbstractTupleIterable(it1.getArity() + it2.getArity()) {
                public TupleCursor tupleCursor() {
                    return new AbstractTupleCursor(getArity()) {
                        final TupleCursor i1 = it1.tupleCursor();
                        final int arity1 = i1.getArity();
                        TupleCursor i2 =
                            TupleCollections.emptyList(1).tupleCursor();
                        final Member[] members = new Member[arity];

                        long currentIteration = 0;
                        Execution execution = Locus.peek().execution;
                        public boolean forward() {
                            if (i2.forward()) {
                                return true;
                            }
                            while (i1.forward()) {
                                CancellationChecker.checkCancelOrTimeout(
                                    currentIteration++, execution);
                                i2 = it2.tupleCursor();
                                if (i2.forward()) {
                                    return true;
                                }
                            }
                            return false;
                        }

                        public List<Member> current() {
                            i1.currentToArray(members, 0);
                            i2.currentToArray(members, arity1);
                            return Util.flatList(members);
                        }

                        @Override
                        public Member member(int column) {
                            if (column < arity1) {
                                return i1.member(column);
                            } else {
                                return i2.member(column - arity1);
                            }
                        }

                        @Override
                        public void setContext(Evaluator evaluator) {
                            i1.setContext(evaluator);
                            i2.setContext(evaluator);
                        }

                        @Override
                        public void currentToArray(
                            Member[] members,
                            int offset)
                        {
                            i1.currentToArray(members, offset);
                            i2.currentToArray(members, offset + arity1);
                        }
                    };
                }
            };
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Immutable List
    ///////////////////////////////////////////////////////////////////////////

    protected ListCalc compileCallImmutableList(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        final ListCalc listCalc1 = toList(compiler, call.getArg(0));
        final ListCalc listCalc2 = toList(compiler, call.getArg(1));
        Calc[] calcs = new Calc[] {listCalc1, listCalc2};
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: LIST or MUTABLE_LIST.
        // Since we want an immutable list as the result, it does not
        // matter whether the Calc list are of type
        // LIST and MUTABLE_LIST - they are treated the same; so
        // there are 4 possible combinations - even sweeter.

        // Check returned calc ResultStyles
        checkListResultStyles(listCalc1);
        checkListResultStyles(listCalc2);

        return new ImmutableListCalc(call, calcs);
    }

    /**
     * Compiles an expression to list (or mutable list) format. Never returns
     * null.
     *
     * @param compiler Compiler
     * @param exp Expression
     * @return Compiled expression that yields a list or mutable list
     */
    private ListCalc toList(ExpCompiler compiler, final Exp exp) {
        // Want immutable list or mutable list in that order
        // It is assumed that an immutable list is easier to get than
        // a mutable list.
        final Type type = exp.getType();
        if (type instanceof SetType) {
            final Calc calc = compiler.compileAs(
                exp, null, ResultStyle.LIST_MUTABLELIST);
            if (calc == null) {
                return compiler.compileList(exp, false);
            }
            return (ListCalc) calc;
        } else {
            return new SetFunDef.SetListCalc(
                new DummyExp(new SetType(type)),
                new Exp[] {exp},
                compiler,
                ResultStyle.LIST_MUTABLELIST);
        }
    }

    abstract class BaseListCalc extends AbstractListCalc {
        protected BaseListCalc(
            ResolvedFunCall call,
            Calc[] calcs,
            boolean mutable)
        {
            super(call, calcs, mutable);
        }

        public TupleList evaluateList(Evaluator evaluator) {
            ResolvedFunCall call = (ResolvedFunCall) exp;
            // Use a native evaluator, if more efficient.
            // TODO: Figure this out at compile time.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            NativeEvaluator nativeEvaluator =
                schemaReader.getNativeSetEvaluator(
                    call.getFunDef(), call.getArgs(), evaluator, this);
            if (nativeEvaluator != null) {
                return (TupleList) nativeEvaluator.execute(ResultStyle.LIST);
            }

            Calc[] calcs = getCalcs();
            ListCalc listCalc1 = (ListCalc) calcs[0];
            ListCalc listCalc2 = (ListCalc) calcs[1];

            TupleList l1 = listCalc1.evaluateList(evaluator);
            TupleList l2 = listCalc2.evaluateList(evaluator);

            l1 = nonEmptyOptimizeList(evaluator, l1, call);
            if (l1.isEmpty()) {
                return TupleCollections.emptyList(
                    l1.getArity() + l2.getArity());
            }
            l2 = nonEmptyOptimizeList(evaluator, l2, call);
            if (l2.isEmpty()) {
                return TupleCollections.emptyList(
                    l1.getArity() + l2.getArity());
            }

            return makeList(l1, l2);
        }

        protected abstract TupleList makeList(TupleList l1, TupleList l2);
    }

    class ImmutableListCalc
        extends BaseListCalc
    {
        ImmutableListCalc(
            ResolvedFunCall call, Calc[] calcs)
        {
            super(call, calcs, false);
        }

        protected TupleList makeList(final TupleList l1, final TupleList l2) {
            final int arity = l1.getArity() + l2.getArity();
            return new DelegatingTupleList(
                arity,
                new AbstractList<List<Member>>() {
                    final List<List<List<Member>>> lists =
                        Arrays.<List<List<Member>>>asList(
                            l1, l2);
                    final Member[] members = new Member[arity];

                    final CartesianProductList cartesianProductList =
                        new CartesianProductList<List<Member>>(
                            lists);

                    @Override
                    public List<Member> get(int index) {
                        cartesianProductList.getIntoArray(index, members);
                        return Util.flatListCopy(members);
                    }

                    @Override
                    public int size() {
                        return cartesianProductList.size();
                    }
                });
        }
    }

    protected ListCalc compileCallMutableList(
        final ResolvedFunCall call,
        ExpCompiler compiler)
    {
        final ListCalc listCalc1 = toList(compiler, call.getArg(0));
        final ListCalc listCalc2 = toList(compiler, call.getArg(1));

        Calc[] calcs = new Calc[] {listCalc1, listCalc2};
        // The Calcs, 1 and 2, can be of type: Member or Member[] and
        // of ResultStyle: LIST or MUTABLE_LIST.
        // Since we want an mutable list as the result, it does not
        // matter whether the Calc list are of type
        // LIST and MUTABLE_LIST - they are treated the same,
        // regardless of type, one must materialize the result list; so
        // there are 4 possible combinations - even sweeter.

        // Check returned calc ResultStyles
        checkListResultStyles(listCalc1);
        checkListResultStyles(listCalc2);

        return new MutableListCalc(call, calcs);
    }

    class MutableListCalc extends BaseListCalc
    {
        MutableListCalc(ResolvedFunCall call, Calc[] calcs)
        {
            super(call, calcs, true);
        }

        @SuppressWarnings({"unchecked"})
        protected TupleList makeList(final TupleList l1, final TupleList l2) {
            final int arity = l1.getArity() + l2.getArity();
            final List<Member> members =
                new ArrayList<Member>(arity * l1.size() * l2.size());
            for (List<Member> ma1 : l1) {
                for (List<Member> ma2 : l2) {
                    members.addAll(ma1);
                    members.addAll(ma2);
                }
            }
            return new ListTupleList(arity, members);
        }
    }

    protected TupleList nonEmptyOptimizeList(
        Evaluator evaluator,
        TupleList list,
        ResolvedFunCall call)
    {
        int opSize = MondrianProperties.instance().CrossJoinOptimizerSize.get();
        if (list.isEmpty()) {
            return list;
        }
        try {
            final Object o = list.get(0);
            if (o instanceof Member) {
                // Cannot optimize high cardinality dimensions
                Dimension dimension = ((Member)o).getDimension();
                if (dimension.isHighCardinality()) {
                    LOGGER.warn(
                        MondrianResource.instance()
                            .HighCardinalityInDimension.str(
                                dimension.getUniqueName()));
                    return list;
                }
            }
        } catch (IndexOutOfBoundsException ioobe) {
            return TupleCollections.emptyList(list.getArity());
        }
        int size = list.size();

        if (size > opSize && evaluator.isNonEmpty()) {
            // instead of overflow exception try to further
            // optimize nonempty(crossjoin(a,b)) ==
            // nonempty(crossjoin(nonempty(a),nonempty(b))
            final int missCount = evaluator.getMissCount();

            list = nonEmptyList(evaluator, list, call);
            size = list.size();
            // list may be empty after nonEmpty optimization
            if (size == 0) {
                return TupleCollections.emptyList(list.getArity());
            }
            final int missCount2 = evaluator.getMissCount();
            final int puntMissCountListSize = 1000;
            if (missCount2 > missCount && size > puntMissCountListSize) {
                // We've hit some cells which are not in the cache. They
                // registered as non-empty, but we won't really know until
                // we've populated the cache. The cartesian product is still
                // huge, so let's quit now, and try again after the cache
                // has been loaded.
                // Return an empty list short circuits higher level
                // evaluation poping one all the way to the top.
                return TupleCollections.emptyList(list.getArity());
            }
        }
        return list;
    }

    public static TupleList mutableCrossJoin(
        TupleList list1,
        TupleList list2)
    {
        return mutableCrossJoin(Arrays.asList(list1, list2));
    }

    public static TupleList mutableCrossJoin(
        List<TupleList> lists)
    {
        long size = 1;
        int arity = 0;
        for (TupleList list : lists) {
            size *= (long) list.size();
            arity += list.getArity();
        }
        if (size == 0L) {
            return TupleCollections.emptyList(arity);
        }

        // Optimize nonempty(crossjoin(a,b)) ==
        //  nonempty(crossjoin(nonempty(a),nonempty(b))

        // FIXME: If we're going to apply a NON EMPTY constraint later, it's
        // possible that the ultimate result will be much smaller.

        Util.checkCJResultLimit(size);

        // Now we can safely cast size to an integer. It still might be very
        // large - which means we're allocating a huge array which we might
        // pare down later by applying NON EMPTY constraints - which is a
        // concern.
        List<Member> result = new ArrayList<Member>((int) size * arity);

        final Member[] partialArray = new Member[arity];
        final List<Member> partial = Arrays.asList(partialArray);
        cartesianProductRecurse(0, lists, partial, partialArray, 0, result);
        return new ListTupleList(arity, result);
    }

    private static void cartesianProductRecurse(
        int i,
        List<TupleList> lists,
        List<Member> partial,
        Member[] partialArray,
        int partialSize,
        List<Member> result)
    {
        final TupleList tupleList = lists.get(i);
        final int partialSizeNext = partialSize + tupleList.getArity();
        final int iNext = i + 1;
        final TupleCursor cursor = tupleList.tupleCursor();
        int currentIteration = 0;
        Execution execution = Locus.peek().execution;
        while (cursor.forward()) {
            CancellationChecker.checkCancelOrTimeout(
                currentIteration++, execution);
            cursor.currentToArray(partialArray, partialSize);
            if (i == lists.size() - 1) {
                result.addAll(partial);
            } else {
                cartesianProductRecurse(
                    iNext, lists, partial, partialArray, partialSizeNext,
                    result);
            }
        }
    }

    /**
     * Traverses the function call tree of
     * the non empty crossjoin function and populates the queryMeasureSet
     * with base measures
     */
    private static class MeasureVisitor extends MdxVisitorImpl {

        private final Set<Member> queryMeasureSet;
        private final ResolvedFunCallFinder finder;
        private final Set<Member> activeMeasures = new HashSet<Member>();

        /**
         * Creates a MeasureVisitor.
         *
         * @param queryMeasureSet Set of measures in query
         *
         * @param crossJoinCall Measures referencing this call should be
         * excluded from the list of measures found
         */
        MeasureVisitor(
            Set<Member> queryMeasureSet,
            ResolvedFunCall crossJoinCall)
        {
            this.queryMeasureSet = queryMeasureSet;
            this.finder = new ResolvedFunCallFinder(crossJoinCall);
        }

        public Object visit(ParameterExpr parameterExpr) {
            final Parameter parameter = parameterExpr.getParameter();
            final Type type = parameter.getType();
            if (type instanceof mondrian.olap.type.MemberType) {
                final Object value = parameter.getValue();
                if (value instanceof Member) {
                    final Member member = (Member) value;
                    process(member);
                }
            }

            return null;
        }

        public Object visit(MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            process(member);
            return null;
        }

        private void process(final Member member) {
            if (member.isMeasure()) {
                if (member.isCalculated()) {
                    if (activeMeasures.add(member)) {
                        Exp exp = member.getExpression();
                        finder.found = false;
                        exp.accept(finder);
                        if (! finder.found) {
                            exp.accept(this);
                        }
                        activeMeasures.remove(member);
                    }
                } else {
                    queryMeasureSet.add(member);
                }
            }
        }
    }

    /**
     * This is the entry point to the crossjoin non-empty optimizer code.
     *
     * <p>What one wants to determine is for each individual Member of the input
     * parameter list, a 'List-Member', whether across a slice there is any
     * data.
     *
     * <p>But what data?
     *
     * <p>For Members other than those in the list, the 'non-List-Members',
     * one wants to consider
     * all data across the scope of these other Members. For instance, if
     * Time is not a List-Member, then one wants to consider data
     * across All Time. Or, if Customer is not a List-Member, then
     * look at data across All Customers. The theory here, is if there
     * is no data for a particular Member of the list where all other
     * Members not part of the list are span their complete hierarchy, then
     * there is certainly no data for Members of that Hierarchy at a
     * more specific Level (more on this below).
     *
     * <p>When a Member that is a non-List-Member is part of a Hierarchy
     * that has an
     * All Member (hasAll="true"), then its very easy to make sure that
     * the All Member is used during the optimization.
     * If a non-List-Member is part of a Hierarchy that does not have
     * an All Member, then one must, in fact, iterate over all top-level
     * Members of the Hierarchy!!! - otherwise a List-Member might
     * be excluded because the optimization code was not looking everywhere.
     *
     * <p>Concerning default Members for those Hierarchies for the
     * non-List-Members, ignore them. What is wanted is either the
     * All Member or one must iterate across all top-level Members, what
     * happens to be the default Member of the Hierarchy is of no relevant.
     *
     * <p>The Measures Hierarchy has special considerations. First, there is
     * no All Measure. But, certainly one need only involve Measures
     * that are actually in the query... yes and no. For Calculated Measures
     * one must also get all of the non-Calculated Measures that make up
     * each Calculated Measure. Thus, one ends up iterating across all
     * Calculated and non-Calculated Measures that are explicitly
     * mentioned in the query as well as all Calculated and non-Calculated
     * Measures that are used to define the Calculated Measures in
     * the query. Why all of these? because this represents the total
     * scope of possible Measures that might yield a non-null value
     * for the List-Members and that is what we what to find. It might
     * be a super set, but thats ok; we just do not want to miss anything.
     *
     * <p>For other Members, the default Member is used, but for Measures one
     * should look for that data for all Measures associated with the query, not
     * just one Measure. For a dense dataset this may not be a problem or even
     * apparent, but for a sparse dataset, the first Measure may, in fact, have
     * not data but other Measures associated with the query might.
     * Hence, the solution here is to identify all Measures associated with the
     * query and then for each Member of the list, determine if there is any
     * data iterating across all Measures until non-null data is found or the
     * end of the Measures is reached.
     *
     * <p>This is a non-optimistic implementation. This means that an
     * element of the input parameter List is only not included in the
     * returned result List if for no combination of Measures, non-All
     * Members (for Hierarchies that have no All Members) and evaluator
     * default Members did the element evaluate to non-null.
     *
     * @param evaluator Evaluator
     *
     * @param list      List of members or tuples
     *
     * @param call      Calling ResolvedFunCall used to determine what Measures
     *                  to use
     *
     * @return List of elements from the input parameter list that have
     * evaluated to non-null.
     */
    protected TupleList nonEmptyList(
        Evaluator evaluator,
        TupleList list,
        ResolvedFunCall call)
    {
        if (list.isEmpty()) {
            return list;
        }

        TupleList result =
            TupleCollections.createList(
                list.getArity(), (list.size() + 2) >> 1);

        // Get all of the Measures
        final Query query = evaluator.getQuery();

        final String measureSetKey = "MEASURE_SET-" + ctag;
        Set<Member> measureSet =
            Util.cast((Set) query.getEvalCache(measureSetKey));

        final String memberSetKey = "MEMBER_SET-" + ctag;
        Set<Member> memberSet =
            Util.cast((Set) query.getEvalCache(memberSetKey));
        // If not in query cache, then create and place into cache.
        // This information is used for each iteration so it makes
        // sense to create and cache it.
        if (measureSet == null || memberSet == null) {
            measureSet = new HashSet<Member>();
            memberSet = new HashSet<Member>();
            Set<Member> queryMeasureSet = query.getMeasuresMembers();
            MeasureVisitor measureVisitor =
                new MeasureVisitor(measureSet, call);

            // MemberExtractingVisitor will collect the dimension members
            // referenced within the measures in the query.
            // One or more measures may conflict with the members in the tuple,
            // overriding the context of the tuple member when determining
            // non-emptiness.
            MemberExtractingVisitor memVisitor =
                new MemberExtractingVisitor(memberSet, call, false);

            for (Member m : queryMeasureSet) {
                if (m.isCalculated()) {
                    Exp exp = m.getExpression();
                    exp.accept(measureVisitor);
                    exp.accept(memVisitor);
                } else {
                    measureSet.add(m);
                }
            }
            Formula[] formula = query.getFormulas();
            if (formula != null) {
                for (Formula f : formula) {
                    if (SqlConstraintUtils.containsValidMeasure(
                            f.getExpression()))
                    {
                        // short circuit if VM is present.
                        return list;
                    }
                    f.accept(measureVisitor);
                }
            }
            query.putEvalCache(measureSetKey, measureSet);
            query.putEvalCache(memberSetKey, memberSet);
        }

        final String allMemberListKey = "ALL_MEMBER_LIST-" + ctag;
        List<Member> allMemberList =
            Util.cast((List) query.getEvalCache(allMemberListKey));

        final String nonAllMembersKey = "NON_ALL_MEMBERS-" + ctag;
        Member[][] nonAllMembers =
            (Member[][]) query.getEvalCache(nonAllMembersKey);
        if (nonAllMembers == null) {
            //
            // Get all of the All Members and those Hierarchies that
            // do not have All Members.
            //
            Member[] evalMembers = evaluator.getMembers().clone();

            List<Member> listMembers = list.get(0);

            // Remove listMembers from evalMembers and independentSlicerMembers
            for (Member lm : listMembers) {
                Hierarchy h = lm.getHierarchy();
                for (int i = 0; i < evalMembers.length; i++) {
                    Member em = evalMembers[i];
                    if ((em != null) && h.equals(em.getHierarchy())) {
                        evalMembers[i] = null;
                    }
                }
            }

            List<Member> slicerMembers = null;
            if (evaluator instanceof RolapEvaluator) {
                RolapEvaluator rev = (RolapEvaluator) evaluator;
                slicerMembers = rev.getSlicerMembers();
            }
            // Iterate the list of slicer members, grouping them by hierarchy
            Map<Hierarchy, Set<Member>> mapOfSlicerMembers =
                new HashMap<Hierarchy, Set<Member>>();
            if (slicerMembers != null) {
                for (Member slicerMember : slicerMembers) {
                    Hierarchy hierarchy = slicerMember.getHierarchy();
                    if (!mapOfSlicerMembers.containsKey(hierarchy)) {
                        mapOfSlicerMembers.put(
                            hierarchy,
                            new HashSet<Member>());
                    }
                    mapOfSlicerMembers.get(hierarchy).add(slicerMember);
                }
            }

            // Now we have the non-List-Members, but some of them may not be
            // All Members (default Member need not be the All Member) and
            // for some Hierarchies there may not be an All Member.
            // So we create an array of Objects some elements of which are
            // All Members and others elements will be an array of all top-level
            // Members when there is not an All Member.
            SchemaReader schemaReader = evaluator.getSchemaReader();
            allMemberList = new ArrayList<Member>();
            List<Member[]> nonAllMemberList = new ArrayList<Member[]>();

            Member em;
            boolean isSlicerMember;
            for (Member evalMember : evalMembers) {
                em = evalMember;

                isSlicerMember =
                    slicerMembers != null
                        && slicerMembers.contains(em);

                if (em == null) {
                    // Above we might have removed some by setting them
                    // to null. These are the CrossJoin axes.
                    continue;
                }
                if (em.isMeasure()) {
                    continue;
                }

                //
                // The unconstrained members need to be replaced by the "All"
                // member based on its usage and property. This is currently
                // also the behavior of native cross join evaluation. See
                // SqlConstraintUtils.addContextConstraint()
                //
                // on slicer? | calculated? | replace with All?
                // -----------------------------------------------
                //     Y      |      Y      |      Y always
                //     Y      |      N      |      N
                //     N      |      Y      |      N
                //     N      |      N      |      Y if not "All"
                // -----------------------------------------------
                //
                if ((isSlicerMember && !em.isCalculated())
                    || (!isSlicerMember && em.isCalculated()))
                {
                    // If the slicer contains multiple members from this one's
                    // hierarchy, add them to nonAllMemberList
                    if (isSlicerMember) {
                        Set<Member> hierarchySlicerMembers =
                            mapOfSlicerMembers.get(em.getHierarchy());
                        if (hierarchySlicerMembers.size() > 1) {
                            nonAllMemberList.add(
                                hierarchySlicerMembers.toArray(
                                    new Member[hierarchySlicerMembers.size()]));
                        }
                    }
                    continue;
                }

                // If the member is not the All member;
                // or if it is a slicer member,
                // replace with the "all" member.
                if (isSlicerMember || !em.isAll()) {
                    Hierarchy h = em.getHierarchy();
                    final List<Member> rootMemberList =
                        schemaReader.getHierarchyRootMembers(h);
                    if (h.hasAll()) {
                        // The Hierarchy has an All member
                        boolean found = false;
                        for (Member m : rootMemberList) {
                            if (m.isAll()) {
                                allMemberList.add(m);
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            LOGGER.warn( "CrossJoinFunDef.nonEmptyListNEW: ERROR" );
                        }
                    } else {
                        // The Hierarchy does NOT have an All member
                        Member[] rootMembers =
                            rootMemberList.toArray(
                                new Member[rootMemberList.size()]);
                        nonAllMemberList.add(rootMembers);
                    }
                }
            }
            nonAllMembers =
                nonAllMemberList.toArray(
                    new Member[nonAllMemberList.size()][]);

            query.putEvalCache(allMemberListKey, allMemberList);
            query.putEvalCache(nonAllMembersKey, nonAllMembers);
        }

        //
        // Determine if there is any data.
        //
        // Put all of the All Members into Evaluator
        final int savepoint = evaluator.savepoint();
        try {
            evaluator.setContext(allMemberList);
            // Iterate over elements of the input list. If for any
            // combination of
            // Measure and non-All Members evaluation is non-null, then
            // add it to the result List.
            final TupleCursor cursor = list.tupleCursor();
            int currentIteration = 0;
            Execution execution = query.getStatement().getCurrentExecution();
            while (cursor.forward()) {
                cursor.setContext(evaluator);
                for (Member member : memberSet) {
                    // memberSet contains members referenced within measures.
                    // Make sure that we don't incorrectly assume a context
                    // that will be changed by the measure, so conservatively
                    // push context to [All] for each of the associated
                    // hierarchies.
                    evaluator.setContext(member.getHierarchy().getAllMember());
                }
                // Check if the MDX query was canceled.
                // Throws an exception in case of timeout is exceeded
                // see MONDRIAN-2425
                CancellationChecker.checkCancelOrTimeout(
                    currentIteration++, execution);
                if (tupleContainsCalcs( cursor.current() ) || checkData(
                        nonAllMembers,
                        nonAllMembers.length - 1,
                        measureSet,
                        evaluator))
                {
                    result.addCurrent(cursor);
                }
            }
            return result;
        } finally {
            evaluator.restore(savepoint);
        }
    }

    private boolean tupleContainsCalcs( List<Member> current ) {
        return current.stream().anyMatch( Member::isCalculated );
    }

    /**
     * Return <code>true</code> if for some combination of Members
     * from the nonAllMembers array of Member arrays and Measures from
     * the Set of Measures evaluate to a non-null value. Even if a
     * particular combination is non-null, all combinations are tested
     * just to make sure that the data is loaded.
     *
     * @param nonAllMembers array of Member arrays of top-level Members
     * for Hierarchies that have no All Member.
     * @param cnt which Member array is to be processed.
     * @param measureSet Set of all that should be tested against.
     * @param evaluator the Evaluator.
     * @return True if at least one combination evaluated to non-null.
     */
    private static boolean checkData(
        Member[][] nonAllMembers,
        int cnt,
        Set<Member> measureSet,
        Evaluator evaluator)
    {
        if (cnt < 0) {
            // no measures found, use standard algorithm
            if (measureSet.isEmpty()) {
                Object value = evaluator.evaluateCurrent();
                if (value != null
                    && !(value instanceof Throwable))
                {
                    return true;
                }
            } else {
                // Here we evaluate across all measures just to
                // make sure that the data is all loaded
                boolean found = false;
                for (Member measure : measureSet) {
                    evaluator.setContext(measure);
                    Object value = evaluator.evaluateCurrent();
                    if (value != null
                        && !(value instanceof Throwable))
                    {
                        found = true;
                    }
                }
                return found;
            }
        } else {
            boolean found = false;
            for (Member m : nonAllMembers[cnt]) {
                evaluator.setContext(m);
                if (checkData(nonAllMembers, cnt - 1, measureSet, evaluator)) {
                    found = true;
                }
            }
            return found;
        }
        return false;
    }

    private static class StarCrossJoinResolver extends MultiResolver {
        public StarCrossJoinResolver() {
            super(
                "*",
                "<Set1> * <Set2>",
                "Returns the cross product of two sets.",
                new String[]{"ixxx", "ixmx", "ixxm", "ixmm"});
        }

        public FunDef resolve(
            Exp[] args,
            Validator validator,
            List<Conversion> conversions)
        {
            // This function only applies in contexts which require a set.
            // Elsewhere, "*" is the multiplication operator.
            // This means that [Measures].[Unit Sales] * [Gender].[M] is
            // well-defined.
            if (validator.requiresExpression()) {
                return null;
            }
            return super.resolve(args, validator, conversions);
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new CrossJoinFunDef(dummyFunDef);
        }
    }
}

// End CrossJoinFunDef.java
