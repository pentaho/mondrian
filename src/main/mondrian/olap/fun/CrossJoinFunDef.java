/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.SetType;
import mondrian.resource.MondrianResource;
import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Definition of the <code>CrossJoin</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class CrossJoinFunDef extends FunDefBase {
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "Crossjoin",
            "Crossjoin(<Set1>, <Set2>)",
            "Returns the cross product of two sets.",
            new String[]{"fxxx"},
            CrossJoinFunDef.class);

    static final StarCrossJoinResolver StarResolver = new StarCrossJoinResolver();

    public CrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        // CROSSJOIN(<Set1>,<Set2>) has type [Hie1] x [Hie2].
        List list = new ArrayList();
        for (int i = 0; i < args.length; i++) {
            Exp arg = args[i];
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
        final Type[] types = (Type[]) list.toArray(new Type[list.size()]);
        final TupleType tupleType = new TupleType(types);
        return new SetType(tupleType);
    }

    /**
     * Adds a type to a list of types. If type is a {@link TupleType}, does so
     * recursively.
     */
    private static void addTypes(final Type type, List list) {
        if (type instanceof SetType) {
            SetType setType = (SetType) type;
            addTypes(setType.getElementType(), list);
        } else if (type instanceof TupleType) {
            TupleType tupleType = (TupleType) type;
            for (int i = 0; i < tupleType.elementTypes.length; i++) {
                addTypes(tupleType.elementTypes[i], list);
            }
        } else {
            list.add(type);
        }
    }

    public Calc compileCall(final ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc1 = toList(compiler, call.getArg(0));
        final ListCalc listCalc2 = toList(compiler, call.getArg(1));
        return new AbstractListCalc(call, new Calc[] {listCalc1, listCalc2}) {
            public List evaluateList(Evaluator evaluator) {
                SchemaReader schemaReader = evaluator.getSchemaReader();
                NativeEvaluator nativeEvaluator =
                        schemaReader.getNativeSetEvaluator(
                                call.getFunDef(), call.getArgs(), evaluator, this);
                if (nativeEvaluator != null) {
                    return (List) nativeEvaluator.execute();
                }

                Evaluator oldEval = null;
                assert (oldEval = evaluator.push()) != null;
                final List list1 = listCalc1.evaluateList(evaluator);
                assert oldEval.equals(evaluator) : "listCalc1 changed context";
                if (list1.isEmpty()) {
                    return Collections.EMPTY_LIST;
                }
                final List list2 = listCalc2.evaluateList(evaluator.push());
                assert oldEval.equals(evaluator) : "listCalc2 changed context";
                return crossJoin(list1, list2, evaluator);
            }
        };
    }

    private ListCalc toList(ExpCompiler compiler, final Exp exp) {
        final Type type = exp.getType();
        if (type instanceof SetType) {
            return compiler.compileList(exp);
        } else {
            return new SetFunDef.SetCalc(
                    new DummyExp(new SetType(type)),
                    new Exp[] {exp},
                    compiler);
        }
    }

    List crossJoin(List list1, List list2, Evaluator evaluator) {
        if (list1.isEmpty() || list2.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        // Optimize nonempty(crossjoin(a,b)) ==
        //  nonempty(crossjoin(nonempty(a),nonempty(b))
        long size = (long)list1.size() * (long)list2.size();
        int resultLimit = MondrianProperties.instance().ResultLimit.get();
        if (size > 1000 && evaluator.isNonEmpty()) {
            // instead of overflow exception try to further
            // optimize nonempty(crossjoin(a,b)) ==
            // nonempty(crossjoin(nonempty(a),nonempty(b))
            final int missCount = evaluator.getMissCount();
            list1 = nonEmptyList(evaluator, list1);
            list2 = nonEmptyList(evaluator, list2);
            size = (long)list1.size() * (long)list2.size();
            // both list1 and list2 may be empty after nonEmpty optimization
            if (size == 0) {
            	return Collections.EMPTY_LIST;
            }
            final int missCount2 = evaluator.getMissCount();
            if (missCount2 > missCount && size > 1000) {
                // We've hit some cells which are not in the cache. They
                // registered as non-empty, but we won't really know until
                // we've populated the cache. The cartesian product is still
                // huge, so let's quit now, and try again after the cache has
                // been loaded.
                return Collections.EMPTY_LIST;
            }
        }

        // Throw an exeption, if the size of the crossjoin exceeds the result
        // limit.
        //
        // FIXME: If we're going to apply a NON EMPTY constraint later, it's
        // possible that the ultimate result will be much smaller.
        if (resultLimit > 0 && resultLimit < size) {
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                            new Long(size), new Long(resultLimit));
        }

        // Throw an exception if the crossjoin exceeds a reasonable limit.
        // (Yes, 4 billion is a reasonable limit.)
        if (size > Integer.MAX_VALUE) {
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                            new Long(size), new Long(Integer.MAX_VALUE));
        }

        // Now we can safely cast size to an integer. It still might be very
        // large - which means we're allocating a huge array which we might
        // pare down later by applying NON EMPTY constraints - which is a
        // concern.
        List result = new ArrayList((int) size);

        boolean neitherSideIsTuple = true;
        int arity0 = 1;
        int arity1 = 1;
        if (list1.get(0) instanceof Member[]) {
            arity0 = ((Member[]) list1.get(0)).length;
            neitherSideIsTuple = false;
        }
        if (list2.get(0) instanceof Member[]) {
            arity1 = ((Member[]) list2.get(0)).length;
            neitherSideIsTuple = false;
        }

        if (neitherSideIsTuple) {
            // Simpler routine if we know neither side contains tuples.
            for (int i = 0, m = list1.size(); i < m; i++) {
                Member o0 = (Member) list1.get(i);
                for (int j = 0, n = list2.size(); j < n; j++) {
                    Member o1 = (Member) list2.get(j);
                    result.add(new Member[]{o0, o1});
                }
            }
        } else {
            // More complex routine if one or both sides are arrays
            // (probably the product of nested CrossJoins).
            Member[] row = new Member[arity0 + arity1];
            for (int i = 0, m = list1.size(); i < m; i++) {
                int x = 0;
                Object o0 = list1.get(i);
                if (o0 instanceof Member) {
                    row[x++] = (Member) o0;
                } else {
                    assertTrue(o0 instanceof Member[]);
                    final Member[] members = (Member[]) o0;
                    for (int k = 0; k < members.length; k++) {
                        row[x++] = members[k];
                    }
                }
                for (int j = 0, n = list2.size(); j < n; j++) {
                    Object o1 = list2.get(j);
                    if (o1 instanceof Member) {
                        row[x++] = (Member) o1;
                    } else {
                        assertTrue(o1 instanceof Member[]);
                        final Member[] members = (Member[]) o1;
                        for (int k = 0; k < members.length; k++) {
                            row[x++] = members[k];
                        }
                    }
                    result.add(row.clone());
                    x = arity0;
                }
            }
        }
        return result;
    }


    /**
     * Visitor which builds a list of all measures referenced in a query.
     */
    static class MeasureVisitor implements mondrian.mdx.MdxVisitor {
        // This set is null unless a measure is found.
        Set measureSet;
        Set queryMeasureSet;
        MeasureVisitor(Set queryMeasureSet) {
            this.queryMeasureSet = queryMeasureSet;
        }
        public Object visit(mondrian.olap.Query query) {
            return null;
        }
        public Object visit(mondrian.olap.QueryAxis queryAxis) {
            return null;
        }
        public Object visit(mondrian.olap.Formula formula) {
            return null;
        }
        public Object visit(mondrian.mdx.UnresolvedFunCall call) {
            return null;
        }
        public Object visit(mondrian.mdx.ResolvedFunCall call) {
            return null;
        }
        public Object visit(mondrian.olap.Id id) {
            return null;
        }
        public Object visit(mondrian.mdx.ParameterExpr parameterExpr) {
            return null;
        }
        public Object visit(mondrian.mdx.DimensionExpr dimensionExpr) {
            return null;
        }
        public Object visit(mondrian.mdx.HierarchyExpr hierarchyExpr) {
            return null;
        }
        public Object visit(mondrian.mdx.LevelExpr levelExpr) {
            return null;
        }
        public Object visit(mondrian.mdx.MemberExpr memberExpr) {
            Member member = memberExpr.getMember();
            Iterator mit = queryMeasureSet.iterator();
            while (mit.hasNext()) {
                Member measure = (Member) mit.next();
                if (measure.equals(member)) {
                    if (measureSet == null) {
                        measureSet = new HashSet();
                    }
                    measureSet.add(measure);
                    break;
                }
            }
            return null;
        }
        public Object visit(mondrian.mdx.NamedSetExpr namedSetExpr) {
            return null;
        }
        public Object visit(mondrian.olap.Literal literal) {
            return null;
        }
    }

    /**
     * What one wants to determine is for each individual Members of the input
     * parameter list whether across a slice there is any data. But what data.
     * For other Members, the default Member is used, but for Measures one
     * should look for that data for all Measures associated with the query, not
     * just one Measure. For a dense dataset this may not be a problem or even
     * apparent, but for a sparse dataset, the first Measure may, in fact, have
     * not data but other Measures associated with the query might.
     * Hence, the solution here is to identify all Measures associated with the
     * query and then for each Member of the list, determine if there is any
     * data iterating across all Measures until non-null data is found or the
     * end of the Measures is reached.
     *
     * @param evaluator
     * @param list
     */
    protected static List nonEmptyList(Evaluator evaluator, List list) {
        if (list.isEmpty()) {
            return list;
        }

        // A compromise between allocating too much and having lots of allocations.
        // If everything is null, then this is too big, but if nothing is null
        // then this results in TWO calls to ArrayList's ensureCapacity method
        // and its associated System.arraycopy method.
        // What is best?
        // Note that an ArrayList does not have an adjustable "growth factor"
        // but rather grows by 1.5.
        List result = new ArrayList((list.size() + 2) >> 1);

        // Get all Measures
        // RME: The only mechanism I could find for getting all Measures
        // associated with the query was to use a the MdxVisitor to get all
        // MemberExprs and test if its Member was one of the Measures.  First,
        // it might be expected that at query parse-time one could determine
        // what Measures were associated with which axes saving the use of the
        // visitor and, many times, this might be true but 2) if the Measures
        // are dynamically generated, for instance using a function such as
        // StrToSet, then one can not count on visiting the axes' Exp and determining
        // all Measures - they can only be known at execution-time. 
        // So, here it is assumed that all Measures are known statically by 
        // this stage of the processing.
        Query query = evaluator.getQuery();
        Set measureSet = null;
        Set queryMeasureSet = query.getMeasuresMembers();
        // if the slicer contains a Measure, then the other axes can not
        // contain a Measure, so look at slicer axis first
        if (queryMeasureSet.size() > 0) {
            MeasureVisitor visitor = new MeasureVisitor(queryMeasureSet);
            QueryAxis[] axes = query.getAxes();
            QueryAxis slicerAxis = query.getSlicerAxis();
            if (slicerAxis != null) {
                slicerAxis.accept(visitor);
            }
            if (visitor.measureSet != null) {
                // Slicer had a Measure, 1) use it and 2) do not need to look at
                // the other axes.
                measureSet = visitor.measureSet;

            } else if (axes.length >  1) {
                for (int i = 0; i < axes.length; i++) {
                    if (axes[i] != null) {
                        axes[i].accept(visitor);
                    }
                }
                // It maybe null, but thats ok here
                measureSet = visitor.measureSet;
            }
        }


        // Determine if there is any data.
        evaluator = evaluator.push();
        if (list.get(0) instanceof Member[]) {
            for (Iterator listItr = list.iterator(); listItr.hasNext();) {
                Member[] ms = (Member[]) listItr.next();
                evaluator.setContext(ms);
                // no measures found, use standard algorithm
                if (measureSet == null) {
                    Object value = evaluator.evaluateCurrent();
                    if (value != null && !(value instanceof Throwable)) {
                        result.add(ms);
                    }
                } else {
                    Iterator measureIter = measureSet.iterator();
                    MEASURES_LOOP:
                    while (measureIter.hasNext()) {
                        Member measure = (Member) measureIter.next();
                        evaluator.setContext(measure);
                        Object value = evaluator.evaluateCurrent();
                        if (value != null && !(value instanceof Throwable)) {
                            result.add(ms);
                            break MEASURES_LOOP;
                        }
                    }
                }
            }
        } else {
            for (Iterator listItr = list.iterator(); listItr.hasNext();) {
                Member m = (Member) listItr.next();
                evaluator.setContext(m);
                // no measures found, use standard algorithm
                if (measureSet == null) {
                    Object value = evaluator.evaluateCurrent();
                    if (value != null && !(value instanceof Throwable)) {
                        result.add(m);
                    }
                } else {
                    Iterator measureIter = measureSet.iterator();
                    measuresLoop:
                    while (measureIter.hasNext()) {
                        Member measure = (Member) measureIter.next();
                        evaluator.setContext(measure);
                        Object value = evaluator.evaluateCurrent();
                        if (value != null && !(value instanceof Throwable)) {
                            result.add(m);
                            break measuresLoop;
                        }
                    }
                }
            }
        }
        return result;
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
                Exp[] args, Validator validator, int[] conversionCount) {
            // This function only applies in contexts which require a set.
            // Elsewhere, "*" is the multiplication operator.
            // This means that [Measures].[Unit Sales] * [Gender].[M] is
            // well-defined.
            if (validator.requiresExpression()) {
                return null;
            }
            return super.resolve(args, validator, conversionCount);
        }

        protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
            return new CrossJoinFunDef(dummyFunDef);
        }
    }
}

// End CrossJoinFunDef.java
