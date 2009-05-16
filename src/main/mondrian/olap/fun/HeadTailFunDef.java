/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2006-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.calc.impl.ConstantCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.Evaluator;
import mondrian.olap.FunDef;
import mondrian.util.UnsupportedList;

import java.util.*;

/**
 * Definition of the <code>Head</code> and <code>Tail</code>
 * MDX builtin functions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class HeadTailFunDef extends FunDefBase {
    static final Resolver TailResolver = new ReflectiveMultiResolver(
            "Tail",
            "Tail(<Set>[, <Count>])",
            "Returns a subset from the end of a set.",
            new String[] {"fxx", "fxxn"},
            HeadTailFunDef.class);

    static final Resolver HeadResolver = new ReflectiveMultiResolver(
            "Head",
            "Head(<Set>[, < Numeric Expression >])",
            "Returns the first specified number of elements in a set.",
            new String[] {"fxx", "fxxn"},
            HeadTailFunDef.class);

    private final boolean head;

    public HeadTailFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
        head = dummyFunDef.getName().equals("Head");
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final ListCalc listCalc =
                compiler.compileList(call.getArg(0));
        final IntegerCalc integerCalc =
                call.getArgCount() > 1 ?
                compiler.compileInteger(call.getArg(1)) :
                ConstantCalc.constantInteger(1);
        if (head) {
            return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    evaluator = evaluator.push(false);
                    List list = listCalc.evaluateList(evaluator);
                    int count = integerCalc.evaluateInteger(evaluator);
                    return head(count, list);
                }
            };
        } else {
            return new AbstractListCalc(call, new Calc[] {listCalc, integerCalc}) {
                public List evaluateList(Evaluator evaluator) {
                    evaluator = evaluator.push(false);
                    List list = listCalc.evaluateList(evaluator);
                    int count = integerCalc.evaluateInteger(evaluator);
                    return tail(count, list);
                }
            };
        }
    }

    static <T> List<T> tail(final int count, final List<T> members) {
        assert members != null;
        final int memberCount = members.size();
        if (count >= memberCount) {
            return members;
        }
        if (count <= 0) {
            return Collections.emptyList();
        }
        return new UnsupportedList<T>() {
            public boolean isEmpty() {
                return count == 0 || members.isEmpty();
            }

            public int size() {
                return Math.min(count, members.size());
            }

            public T get(final int idx) {
                final int index = idx + memberCount - count;
                return members.get(index);
            }

            public Iterator<T> iterator() {
                return new ItrUnknownSize();
            }

            public Object[] toArray() {
                final int offset = memberCount - count;
                final Object[] a = new Object[size()];
                for (int i = memberCount - count; i < memberCount; i++) {
                    a[i - offset] = members.get(i);
                }
                return a;
            }
        };
    }

    static <T> List<T> head(final int count, final List<T> members) {
        assert members != null;
        if (count <= 0) {
            return Collections.emptyList();
        }
        return new UnsupportedList<T>() {
            public boolean isEmpty() {
                return count == 0 || members.isEmpty();
            }

            public int size() {
                return Math.min(count, members.size());
            }

            public T get(final int index) {
                if (index >= count) {
                    throw new IndexOutOfBoundsException();
                }
                return members.get(index);
            }

            public Iterator<T> iterator() {
                return new ItrUnknownSize();
            }

            public Object[] toArray() {
                Object[] a = new Object[count];
                int i = 0;
                for (Object member : members) {
                    if (i >= a.length) {
                        return a;
                    }
                    a[i++] = member;
                }
                if (i < a.length) {
                    Object[] a0 = a;
                    a = new Object[i];
                    System.arraycopy(a0, 0, a, 0, i);
                }
                return a;
            }
        };
    }
}

// End HeadTailFunDef.java
