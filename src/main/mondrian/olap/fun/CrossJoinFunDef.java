/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2004-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.TupleType;
import mondrian.olap.type.SetType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Definition of the <code>CROSSJOIN</code> MDX function.
 */
class CrossJoinFunDef extends FunDefBase {
    public CrossJoinFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Type getResultType(Validator validator, Exp[] args) {
        // CROSSJOIN(<Set1>,<Set2>) has type [Hie1] x [Hie2].
        ArrayList list = new ArrayList();
        for (int i = 0; i < args.length; i++) {
            Exp arg = args[i];
            final Type type = arg.getTypeX();
            if (type instanceof SetType) {
                SetType setType = (SetType) type;
                addTypes(setType.getElementType(), list);
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
    private static void addTypes(final Type type, ArrayList list) {
        if (type instanceof TupleType) {
                TupleType tupleType = (TupleType) type;
            for (int i = 0; i < tupleType.elementTypes.length; i++) {
                addTypes(tupleType.elementTypes[i], list);
            }
        } else {
            list.add(type);
        }
    }

    public Object evaluate(Evaluator evaluator, Exp[] args) {
        List set0 = getArgAsList(evaluator, args, 0);
        List set1 = getArgAsList(evaluator, args, 1);

        // optimize nonempty(crossjoin(a,b)) ==
        //  nonempty(crossjoin(nonempty(a),nonempty(b))

        long size = (long)set0.size() * (long)set1.size();
        if (size > 1000 && evaluator.isNonEmpty()) {
            set0 = nonEmptyList(evaluator, set0);
            set1 = nonEmptyList(evaluator, set1);
            size = (long)set0.size() * (long)set1.size();
        }

        if (set0.isEmpty() || set1.isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        // throw an exeption, if the crossjoin gets too large
        int limit = MondrianProperties.instance().getResultLimit();
        if (limit > 0 && limit < size) {
            // result limit exceeded, throw an exception
            throw MondrianResource.instance().
                    newLimitExceededDuringCrossjoin(
                            new Long(size), new Long(limit));
        }

        boolean neitherSideIsTuple = true;
        int arity0 = 1;
        int arity1 = 1;
        if (set0.get(0) instanceof Member[]) {
            arity0 = ((Member[]) set0.get(0)).length;
            neitherSideIsTuple = false;
        }
        if (set1.get(0) instanceof Member[]) {
            arity1 = ((Member[]) set1.get(0)).length;
            neitherSideIsTuple = false;
        }
        List result = new ArrayList();
        if (neitherSideIsTuple) {
            // Simpler routine if we know neither side contains tuples.
            for (int i = 0, m = set0.size(); i < m; i++) {
                Member o0 = (Member) set0.get(i);
                for (int j = 0, n = set1.size(); j < n; j++) {
                    Member o1 = (Member) set1.get(j);
                    result.add(new Member[]{o0, o1});
                }
            }
        } else {
            // More complex routine if one or both sides are arrays
            // (probably the product of nested CrossJoins).
            Member[] row = new Member[arity0 + arity1];
            for (int i = 0, m = set0.size(); i < m; i++) {
                int x = 0;
                Object o0 = set0.get(i);
                if (o0 instanceof Member) {
                    row[x++] = (Member) o0;
                } else {
                    assertTrue(o0 instanceof Member[]);
                    final Member[] members = (Member[]) o0;
                    for (int k = 0; k < members.length; k++) {
                        row[x++] = members[k];
                    }
                }
                for (int j = 0, n = set1.size(); j < n; j++) {
                    Object o1 = set1.get(j);
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

    private static List getArgAsList(Evaluator evaluator, Exp[] args,
            int index) {
        final Object arg = getArg(evaluator, args, index);
        if (arg instanceof List) {
            return (List) arg;
        } else {
            List list = new ArrayList();
            list.add(arg);
            return list;
        }
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
                if (value != Util.nullValue && !(value instanceof Throwable)) {
                    result.add(m);
                }
            }
        } else {
            for (Iterator it = list.iterator(); it.hasNext();) {
                Member m = (Member) it.next();
                evaluator.setContext(m);
                Object value = evaluator.evaluateCurrent();
                if (value != Util.nullValue && !(value instanceof Throwable)) {
                    result.add(m);
                }
            }
        }
        return result;
    }

}

// End CrossJoinFunDef.java
