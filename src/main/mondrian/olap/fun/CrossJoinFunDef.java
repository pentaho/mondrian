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
import java.util.Iterator;
import java.util.List;

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
        //if (resultLimit > 0 && size > resultLimit && evaluator.isNonEmpty()) {
        if (size > 1000 && evaluator.isNonEmpty()) {
            // instead of overflow exception try to further
            // optimize nonempty(crossjoin(a,b)) ==
            // nonempty(crossjoin(nonempty(a),nonempty(b))
            final int missCount = evaluator.getMissCount();
            list1 = nonEmptyList(evaluator, list1);
            list2 = nonEmptyList(evaluator, list2);
            size = (long)list1.size() * (long)list2.size();
            // both list1 and list2 may be empty after nonEmpty optimization
            if (size == 0)
            	return Collections.EMPTY_LIST;
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

        // throw an exeption, if the crossjoin gets too large
        if (resultLimit > 0 && resultLimit < size) {
            // result limit exceeded, throw an exception
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                            new Long(size), new Long(resultLimit));
        }

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

        if (size > Integer.MAX_VALUE) {
            // If the long "size" value is greater than Integer.MAX_VALUE, then
            // it can not be used as the size for an array allocation.
            String msg = "Union size \"" + 
                size + 
                "\" too big (greater than Integer.MAX_VALUE)";
            throw Util.newInternal(msg);
        }
        List result = new ArrayList((int) size);

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

    protected static List nonEmptyList(Evaluator evaluator, List list) {
        if (list.isEmpty()) {
            return list;
        }
        List result = new ArrayList();
        evaluator = evaluator.push();
        if (list.get(0) instanceof Member[]) {
            for (Iterator it = list.iterator(); it.hasNext();) {
                Member[] m = (Member[]) it.next();
                evaluator.setContext(m);
                Object value = evaluator.evaluateCurrent();
                if (value != null && !(value instanceof Throwable)) {
                    result.add(m);
                }
            }
        } else {
            for (Iterator it = list.iterator(); it.hasNext();) {
                Member m = (Member) it.next();
                evaluator.setContext(m);
                Object value = evaluator.evaluateCurrent();
                if (value != null && !(value instanceof Throwable)) {
                    result.add(m);
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
