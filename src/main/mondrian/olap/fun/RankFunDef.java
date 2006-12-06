/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.*;
import mondrian.calc.*;
import mondrian.calc.impl.*;
import mondrian.rolap.RolapUtil;
import mondrian.mdx.ResolvedFunCall;

import java.util.*;
import java.io.PrintWriter;

/**
 * Definition of the <code>RANK</code> MDX function.
 *
 * @author Richard Emberson
 * @since 17 January, 2005
 * @version $Id$
 */
public class RankFunDef extends FunDefBase {
    static final boolean debug = false;
    static final ReflectiveMultiResolver Resolver = new ReflectiveMultiResolver(
            "Rank",
            "Rank(<Tuple>, <Set> [, <Calc Expression>])",
            "Returns the one-based rank of a tuple in a set.",
            new String[]{"fitx", "fitxn", "fimx", "fimxn"},
            RankFunDef.class);

    public RankFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        switch (call.getArgCount()) {
        case 2:
            return compileCall2(call, compiler);
        case 3:
            return compileCall3(call, compiler);
        default:
            throw Util.newInternal("invalid arg count " + call.getArgCount());
        }
    }

    public Calc compileCall3(ResolvedFunCall call, ExpCompiler compiler) {
        final Type type0 = call.getArg(0).getType();
        if (type0 instanceof TupleType) {
            final TupleCalc tupleCalc =
                    compiler.compileTuple(call.getArg(0));
            final ListCalc listCalc =
                    compiler.compileList(call.getArg(1));
            final Calc sortCalc =
                    compiler.compileScalar(call.getArg(2), true);
            Calc sortedListCalc =
                    new SortCalc(call, listCalc, sortCalc);
            final ExpCacheDescriptor cacheDescriptor =
                    new ExpCacheDescriptor(
                            call, sortedListCalc, compiler.getEvaluator());
            return new Rank3TupleCalc(call, tupleCalc, sortCalc, cacheDescriptor);
        } else {
            final MemberCalc memberCalc =
                    compiler.compileMember(call.getArg(0));
            final ListCalc listCalc = compiler.compileList(call.getArg(1));
            final Calc sortCalc = compiler.compileScalar(call.getArg(2), true);
            Calc sortedListCalc =
                    new SortCalc(call, listCalc, sortCalc);
            final ExpCacheDescriptor cacheDescriptor =
                    new ExpCacheDescriptor(
                            call, sortedListCalc, compiler.getEvaluator());
            return new Rank3MemberCalc(call, memberCalc, sortCalc, cacheDescriptor);
        }
    }

    public Calc compileCall2(ResolvedFunCall call, ExpCompiler compiler) {
        final Exp listExp = call.getArg(1);
        ListCalc listCalc0 = compiler.compileList(listExp);
        Calc listCalc1 = new RankedListCalc(listCalc0);
        final Calc listCalc;
        if (MondrianProperties.instance().EnableExpCache.get()) {
            final ExpCacheDescriptor key = new ExpCacheDescriptor(
                    listExp, listCalc1, compiler.getEvaluator());
            listCalc = new CacheCalc(listExp, key);
        } else {
            listCalc = listCalc1;
        }
        if (call.getArg(0).getType() instanceof TupleType) {
            final TupleCalc tupleCalc =
                    compiler.compileTuple(call.getArg(0));
            return new Rank2TupleCalc(call, tupleCalc, listCalc);
        } else {
            final MemberCalc memberCalc =
                    compiler.compileMember(call.getArg(0));
            return new Rank2MemberCalc(call, memberCalc, listCalc);
        }
    }

    private static class Rank2TupleCalc extends AbstractDoubleCalc {
        private final TupleCalc tupleCalc;
        private final Calc listCalc;

        public Rank2TupleCalc(ResolvedFunCall call, TupleCalc tupleCalc, Calc listCalc) {
            super(call, new Calc[] {tupleCalc, listCalc});
            this.tupleCalc = tupleCalc;
            this.listCalc = listCalc;
        }

        public double evaluateDouble(Evaluator evaluator) {
            // Get member or tuple.
            // If the member is null (or the tuple contains a null member)
            // the result is null (even if the list is null).
            final Member[] members = tupleCalc.evaluateTuple(evaluator);
            if (members == null) {
                return DoubleNull;
            }
            assert !tupleContainsNullMember(members);

            // Get the set of members/tuples.
            // If the list is empty, MSAS cannot figure out the type of the
            // list, so returns an error "Formula error - dimension count is
            // not valid - in the Rank function". We will naturally return 0,
            // which I think is better.
            RankedList rankedList = (RankedList) listCalc.evaluate(evaluator);
            if (rankedList == null) {
                return 0;
            }

            // Find position of member in list. -1 signifies not found.
            final int i = rankedList.indexOf(members);
            // Return 1-based rank. 0 signifies not found.
            return i + 1;
        }
    }

    private static class Rank2MemberCalc extends AbstractDoubleCalc {
        private final MemberCalc memberCalc;
        private final Calc listCalc;

        public Rank2MemberCalc(ResolvedFunCall call, MemberCalc memberCalc, Calc listCalc) {
            super(call, new Calc[] {memberCalc, listCalc});
            this.memberCalc = memberCalc;
            this.listCalc = listCalc;
        }

        public double evaluateDouble(Evaluator evaluator) {
            // Get member or tuple.
            // If the member is null (or the tuple contains a null member)
            // the result is null (even if the list is null).
            final Member member = memberCalc.evaluateMember(evaluator);
            if (member == null ||
                    member.isNull()) {
                return DoubleNull;
            }
            // Get the set of members/tuples.
            // If the list is empty, MSAS cannot figure out the type of the
            // list, so returns an error "Formula error - dimension count is
            // not valid - in the Rank function". We will naturally return 0,
            // which I think is better.
            RankedList rankedList = (RankedList) listCalc.evaluate(evaluator);
            if (rankedList == null) {
                return 0;
            }

            // Find position of member in list. -1 signifies not found.
            final int i = rankedList.indexOf(member);
            // Return 1-based rank. 0 signifies not found.
            return i + 1;
        }
    }

    private static class Rank3TupleCalc extends AbstractDoubleCalc {
        private final TupleCalc tupleCalc;
        private final Calc sortCalc;
        private final ExpCacheDescriptor cacheDescriptor;

        public Rank3TupleCalc(
                ResolvedFunCall call,
                TupleCalc tupleCalc,
                Calc sortCalc,
                ExpCacheDescriptor cacheDescriptor) {
            super(call, new Calc[] {tupleCalc, sortCalc});
            this.tupleCalc = tupleCalc;
            this.sortCalc = sortCalc;
            this.cacheDescriptor = cacheDescriptor;
        }

        public double evaluateDouble(Evaluator evaluator) {
            Member[] members = tupleCalc.evaluateTuple(evaluator);
            if (members == null) {
                return DoubleNull;
            }
            assert !tupleContainsNullMember(members);

            // Compute the value of the tuple.
            final Evaluator evaluator2 = evaluator.push(members);
            Object value = sortCalc.evaluate(evaluator2);
            if (value instanceof RuntimeException) {
                // The value wasn't ready, so quit now... we'll be back.
                return 0;
            }

            // Evaluate the list (or retrieve from cache).
            // If there was an exception while calculating the
            // list, propagate it up.
            final SortResult sortResult = (SortResult)
                    evaluator.getCachedResult(cacheDescriptor);
            if (debug) {
                sortResult.print(new PrintWriter(System.out));
            }
            if (sortResult.empty) {
                // If list is empty, the rank is null.
                return DoubleNull;
            }

            // If value is null, it won't be in the values array.
            if (value == Util.nullValue) {
                return sortResult.values.length + 1;
            }
            // Look for the ranked value in the array.
            int j = FunUtil.searchValuesDesc(sortResult.values, value);
            if (j < 0) {
                // Value not found. Flip the result to find the
                // insertion point.
                j = -(j + 1);
                return j + 1; // 1-based
            }
            if (j <= sortResult.values.length) {
                // If the values preceding are equal, increase the rank.
                while (j > 0 && sortResult.values[j - 1].equals(value)) {
                    --j;
                }
            }
            return j + 1; // 1-based
        }
    }

    private static class Rank3MemberCalc extends AbstractDoubleCalc {
        private final MemberCalc memberCalc;
        private final Calc sortCalc;
        private final ExpCacheDescriptor cacheDescriptor;

        public Rank3MemberCalc(
                ResolvedFunCall call,
                MemberCalc memberCalc,
                Calc sortCalc,
                ExpCacheDescriptor cacheDescriptor) {
            super(call, new Calc[] {memberCalc, sortCalc});
            this.memberCalc = memberCalc;
            this.sortCalc = sortCalc;
            this.cacheDescriptor = cacheDescriptor;
        }

        public double evaluateDouble(Evaluator evaluator) {
            Member member = memberCalc.evaluateMember(evaluator);
            if (member == null || member.isNull()) {
                return DoubleNull;
            }
            // Compute the value of the tuple.
            final Evaluator evaluator2 = evaluator.push(member);
            Object value = sortCalc.evaluate(evaluator2);
            if (value == RolapUtil.valueNotReadyException) {
                // The value wasn't ready, so quit now... we'll be back.
                return 0;
            }

            // Evaluate the list (or retrieve from cache).
            // If there was an exception while calculating the
            // list, propagate it up.
            final SortResult sortResult = (SortResult)
                    evaluator.getCachedResult(cacheDescriptor);
            if (debug) {
                sortResult.print(new PrintWriter(System.out));
            }
            if (sortResult.empty) {
                // If list is empty, the rank is null.
                return DoubleNull;
            }

            // If value is null, it won't be in the values array.
            if (value == Util.nullValue) {
                return sortResult.values.length + 1;
            }
            // Look for the ranked value in the array.
            int j = FunUtil.searchValuesDesc(sortResult.values, value);
            if (j < 0) {
                // Value not found. Flip the result to find the
                // insertion point.
                j = -(j + 1);
                return j + 1; // 1-based
            }
            if (j <= sortResult.values.length) {
                // If the values preceding are equal, increase the rank.
                while (j > 0 && sortResult.values[j - 1].equals(value)) {
                    --j;
                }
            }
            return j + 1; // 1-based
        }
    }

    /**
     * Expression which evaluates an expression to form a list of tuples,
     * evaluates a scalar expression at each tuple, then sorts the list of
     * values. The result is a value of type {@link SortResult}, and can be
     * used to implement the <code>Rank</code> function efficiently.
     */
    private static class SortCalc extends AbstractCalc {
        private final ListCalc listCalc;
        private final Calc sortCalc;

        public SortCalc(Exp exp, ListCalc listExp, Calc sortExp) {
            super(exp);
            this.listCalc = listExp;
            this.sortCalc = sortExp;
        }

        public Calc[] getCalcs() {
            return new Calc[] {listCalc, sortCalc};
        }

        public boolean dependsOn(Dimension dimension) {
            return anyDependsButFirst(getCalcs(), dimension);
        }

        public Object evaluate(Evaluator evaluator) {
            // Create a new evaluator so we don't corrupt the given one.
            final Evaluator evaluator2 = evaluator.push();
            // Construct an array containing the value of the expression
            // for each member.
            List members = (List) listCalc.evaluate(evaluator2);
            assert members != null;
            if (members.isEmpty()) {
                return new SortResult(true, new Object[0]);
            }
            RuntimeException exception = null;
            Object[] values = new Object[members.size()];
            int j = 0;
            for (Object member1 : members) {
                final Object o = member1;
                if (o instanceof Member) {
                    Member member = (Member) o;
                    evaluator2.setContext(member);
                } else {
                    evaluator2.setContext((Member[]) o);
                }
                final Object value = sortCalc.evaluate(evaluator2);
                if (value instanceof RuntimeException) {
                    if (exception == null) {
                        exception = (RuntimeException) value;
                    }
                } else if (Util.isNull(value)) {
                    ;
                } else {
                    values[j++] = value;
                }
            }
            // If there were exceptions, quit now... we'll be back.
            if (exception != null) {
                return exception;
            }
            // If the array is shorter than we expected (because of null
            // values) truncate it.
            if (j < members.size()) {
                final Object[] oldValues = values;
                values = new Object[j];
                System.arraycopy(oldValues, 0, values, 0, j);
            }
            // Sort the array.
            FunUtil.sortValuesDesc(values);
            return new SortResult(false, values);
        }
    }

    /**
     * Holder for the result of sorting a set of values.
     *
     * <p>todo: More optimal representation if a lot of the values are the
     * same.
     */
    private static class SortResult {
        /**
         * Whether the list of tuples was empty.
         * If this is the case, the rank will always be null.
         *
         * <p>It's possible for there to be a positive number of tuples, all
         * of whose values are null, in which case, empty will be false but
         * values will be empty.
         */
        final boolean empty;
        /**
         * Values in sorted order. Null values are not present: they would
         * be at the end, anyway.
         */
        final Object[] values;

        public SortResult(boolean empty, Object[] values) {
            this.empty = empty;
            this.values = values;
            assert values != null;
            assert !empty || values.length == 0;
        }

        public void print(PrintWriter pw) {
            if (empty) {
                pw.println("SortResult: empty");
            } else {
                pw.println("SortResult {");
                for (int i = 0; i < values.length; i++) {
                    if (i > 0) {
                        pw.println(",");
                    }
                    Object value = values[i];
                    pw.print(value);
                }
                pw.println("}");
            }
            pw.flush();
        }
    }

    /**
     * Expression which evaluates an expression to form a list of tuples.
     * The result is a value of type {@link RankedList}, or null if the list
     * is empty.
     */
    private static class RankedListCalc extends AbstractCalc {
        private final ListCalc listCalc;

        public RankedListCalc(ListCalc listCalc) {
            super(new DummyExp(listCalc.getType()));
            this.listCalc = listCalc;
        }

        public Calc[] getCalcs() {
            return new Calc[] {listCalc};
        }

        public Object evaluate(Evaluator evaluator) {
            // Construct an array containing the value of the expression
            // for each member.
            List members = listCalc.evaluateList(evaluator);
            if (members == null) {
                return null;
            }
            return new RankedList(members);
        }
    }

    /**
     * Data structure which contains a list and can return the position of an
     * element in the list in O(log N).
     */
    static class RankedList {
        Map<Object, Integer> map = new HashMap<Object, Integer>();

        RankedList(List members) {
            for (int i = 0; i < members.size(); i++) {
                Object o = members.get(i);
                final Object key;
                if (o instanceof Member) {
                    key = o;
                } else if (o instanceof Member[]) {
                    key = Arrays.asList((Member []) o);
                } else {
                    throw Util.newInternal("bad member/tuple " + o);
                }
                final Integer value = map.put(key, i);
                if (value != null) {
                    // The list already contained a value for this key -- put
                    // it back.
                    map.put(key, value);
                }
            }
        }

        int indexOf(Member m) {
            return indexOf((Object) m);
        }

        int indexOf(Member[] tuple) {
            return indexOf(Arrays.asList(tuple));
        }

        private int indexOf(Object o) {
            Integer integer = map.get(o);
            if (integer == null) {
                return -1;
            } else {
                return integer;
            }
        }
    }
}

// End RankFunDef.java
