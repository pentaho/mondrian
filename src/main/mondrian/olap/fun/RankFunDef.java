/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.olap.fun;

import mondrian.olap.*;
import mondrian.olap.type.Type;
import mondrian.olap.type.NumericType;

import java.util.*;
import java.io.PrintWriter;

/**
 * Definition of the <code>RANK</code> MDX function.
 *
 * @author Richard Emberson
 * @since 17 January, 2005
 * @version $Id$
 */
public abstract class RankFunDef extends FunDefBase {
    public RankFunDef(FunDef dummyFunDef) {
        super(dummyFunDef);
    }

    protected Exp validateArg(
            Validator validator, FunCall call, int i, int type) {
        // If this is the two-argument form of the function,
        // and if expression cache is enabled,
        // wrap second argument (the set)
        // in a function which will use the expression cache.
        if (call.getArgCount() == 2 && i == 1) {
            Exp arg = call.getArgs()[1];
            RankedListExp rankedListExp = new RankedListExp(arg);
            if (MondrianProperties.instance().EnableExpCache.get()) {
                final Exp cacheCall = new FunCall(
                        "$Cache",
                        Syntax.Internal,
                        new Exp[] {
                            rankedListExp
                        });
                return validator.validate(cacheCall, false);
            } else {
                return validator.validate(rankedListExp, false);
            }
        }
        return super.validateArg(validator, call, i, type);
    }

    /**
     * Returns whether two tuples are equal.
     */
    static boolean equalTuple(Member[] tuple, Member[] m) {
        if (tuple.length != m.length) {
            return false;
        }
        for (int i = 0; i < tuple.length; i++) {
            if (! tuple[i].equals(m[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether one of the members in a tuple is null.
     */
    static boolean tupleContainsNullMember(Member[] tuple) {
        for (int i = 0; i < tuple.length; i++) {
            // Rank of a null member or partially null tuple returns null.
            Member member = tuple[i];
            if (member.isNull()) {
                return true;
            }
        }
        return false;
    }

    public static MultiResolver createResolver() {
        return new MultiResolver(
                "Rank",
                "Rank(<Tuple>, <Set> [, <Calc Expression>])",
                "Returns the one-based rank of a tuple in a set.",
                new String[]{"fitx","fitxn", "fimx", "fimxn"}) {
            protected FunDef createFunDef(Exp[] args, FunDef dummyFunDef) {
                switch (args.length) {
                case 2:
                    return new Rank2FunDef(dummyFunDef);
                case 3:
                    return new Rank3FunDef(dummyFunDef);
                default:
                    throw Util.newInternal("invalid arg count " + args.length);
                }
            }
        };
    }

    /**
     * Rank function with 2 arguments:
     *
     * {@code Rank(<<Tuple>>, <<Set>>)}
     */
    private static class Rank2FunDef extends RankFunDef {
        public Rank2FunDef(FunDef dummyFunDef) {
            super(dummyFunDef);
        }

        public Object evaluate(Evaluator evaluator, Exp[] args) {
            // Get member or tuple.
            // If the member is null (or the tuple contains a null member)
            // the result is null (even if the list is null).
            Exp arg = args[0];
            Object o = arg.evaluate(evaluator);
            if (o == null ||
                    o instanceof Member &&
                    ((Member) o).isNull() ||
                    o instanceof Member[] &&
                    tupleContainsNullMember((Member[]) o)) {
                return null;
            }
            // Get the set of members/tuples.
            // If the list is empty, MSAS cannot figure out the type of the
            // list, so returns an error "Formula error - dimension count is
            // not valid - in the Rank function". We will naturally return 0,
            // which I think is better.
            RankedList rankedList = (RankedList) getArg(evaluator, args, 1);
            if (rankedList == null) {
                return new Double(0);
            }

            if (o instanceof Member[]) {
                Member[] tuple = (Member[]) o;
                if (tupleContainsNullMember(tuple)) {
                    return null;
                }
                // Find position of member in list. -1 signifies not found.
                final int i = rankedList.indexOf(tuple);
                // Return 1-based rank. 0 signifies not found.
                return new Double(i + 1);
            } else if (o instanceof Member) {
                Member member = (Member) o;
                if (member.isNull()) {
                    return null;
                }

                // Find position of member in list. -1 signifies not found.
                final int i = rankedList.indexOf(member);
                // Return 1-based rank. 0 signifies not found.
                return new Double(i + 1);
            } else {
                throw Util.newInternal("Expected tuple or member, got " + o);
            }
        }
    }

    /**
     * Rank function with 3 arguments:
     *
     * {@code Rank(<<Tuple>>, <<Set>>, <<Calc Expression>>)}
     */
    private static class Rank3FunDef extends RankFunDef {
        private ExpCacheDescriptor cacheDescriptor;
        private static final boolean debug = false;

        public Rank3FunDef(FunDef dummyFunDef) {
            super(dummyFunDef);
        }

        public Object evaluate(Evaluator evaluator, Exp[] args) {
            // get tuple
            Member[] tuple = getTupleOrMemberArg(evaluator, args, 0);
            if (tuple == null ||
                    tupleContainsNullMember(tuple)) {
                return null;
            }

            // Compute the value of the tuple.
            final Evaluator evaluator2 = evaluator.push(tuple);
            final Exp sortExp = args[2];
            Object value = sortExp.evaluateScalar(evaluator2);
            if (value instanceof RuntimeException) {
                // The value wasn't ready, so quit now... we'll be back.
                return value;
            }

            // Evaluate the list (or retrieve from cache).
            if (cacheDescriptor == null) {
                final Exp listExp = args[1];
                final BuiltinFunTable funTable = BuiltinFunTable.instance();
                Exp sortedListExp = new SortExp(
                        listExp,
                        funTable.createValueFunCall(
                                sortExp,
                                Util.createSimpleValidator(funTable)));
                cacheDescriptor = new ExpCacheDescriptor(sortedListExp, evaluator);
            }
            final Object cachedResult =
                    evaluator.getCachedResult(cacheDescriptor);
            // If there was an exception while calculating the list, propagate
            // it up.
            if (cachedResult instanceof RuntimeException) {
                return (RuntimeException) cachedResult;
            }
            final SortResult sortResult = (SortResult) cachedResult;
            if (debug) {
                sortResult.print(new PrintWriter(System.out));
            }
            if (sortResult.empty) {
                // If list is empty, the rank is null.
                return Util.nullValue;
            }

            // If value is null, it won't be in the values array.
            if (value == Util.nullValue) {
                return new Double(sortResult.values.length + 1);
            }
            // Look for the ranked value in the array.
            int j = FunUtil.searchValuesDesc(sortResult.values, value);
            if (j < 0) {
                // Value not found. Flip the result to find the insertion point.
                j = -(j + 1);
                return new Double(j + 1); // 1-based
            }
            if (j <= sortResult.values.length) {
                // If the values preceding are equal, increase the rank.
                while (j > 0 && sortResult.values[j - 1].equals(value)) {
                    --j;
                }
            }
            return new Double(j + 1); // 1-based
        }
    }

    /**
     * Expression which evaluates an expression to form a list of tuples,
     * evaluates a scalar expression at each tuple, then sorts the list of
     * values. The result is a value of type {@link SortResult}, and can be
     * used to implement the <code>Rank</code> function efficiently.
     */
    private static class SortExp extends ExpBase {
        private final Exp listExp;
        private final Exp sortExp;

        public SortExp(Exp listExp, Exp sortExp) {
            this.listExp = listExp;
            this.sortExp = sortExp;
        }

        public Object clone() {
            return this;
        }

        public int getCategory() {
            throw new UnsupportedOperationException();
        }

        public Type getTypeX() {
            // white lie -- the answer is not important
            return new NumericType();
        }

        public Exp accept(Validator validator) {
            return this;
        }

        public boolean dependsOn(Dimension dimension) {
            // Similar dependency pattern to ORDER function (qv).
            // Depends upon everything listExp and sortExp depend upon, except
            // the dimensions of listExp.
            if (listExp.dependsOn(dimension)) {
                return true;
            }
            if (listExp.getTypeX().usesDimension(dimension)) {
                return false;
            }
            return sortExp.dependsOn(dimension);
        }

        public Object evaluate(Evaluator evaluator) {
            // Create a new evaluator so we don't corrupt the given one.
            final Evaluator evaluator2 = evaluator.push();
            // Construct an array containing the value of the expression
            // for each member.
            List members = (List) listExp.evaluate(evaluator2);
            if (members == null) {
                return new SortResult(true, null);
            }
            RuntimeException exception = null;
            Object[] values = new Object[members.size()];
            int j = 0;
            for (int i = 0; i < members.size(); i++) {
                final Object o = members.get(i);
                if (o instanceof Member) {
                    Member member = (Member) o;
                    evaluator2.setContext(member);
                } else {
                    evaluator2.setContext((Member[]) o);
                }
                final Object value = sortExp.evaluateScalar(evaluator2);
                if (value instanceof RuntimeException) {
                    if (exception == null) {
                        exception = (RuntimeException) value;
                    }
                } else if (value == Util.nullValue) {
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
    private static class RankedListExp extends ExpBase {
        private final Exp listExp;

        public RankedListExp(Exp listExp) {
            this.listExp = listExp;
        }

        public Object clone() {
            return this;
        }

        public int getCategory() {
            return listExp.getCategory();
        }

        public Type getTypeX() {
            return listExp.getTypeX();
        }

        public Exp accept(Validator validator) {
            return this;
        }

        public boolean dependsOn(Dimension dimension) {
            return listExp.dependsOn(dimension);
        }

        public Object evaluate(Evaluator evaluator) {
            // Construct an array containing the value of the expression
            // for each member.
            List members = (List) listExp.evaluate(evaluator);
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
        Map map = new HashMap();

        RankedList(List members) {
            for (int i = 0; i < members.size(); i++) {
                Object o = (Object) members.get(i);
                final Object key;
                if (o instanceof Member) {
                    key = o;
                } else if (o instanceof Member[]) {
                    key = Arrays.asList((Member []) o);
                } else {
                    throw Util.newInternal("bad member/tuple " + o);
                }
                final Object value = map.put(key, new Integer(i));
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
            Integer integer = (Integer) map.get(o);
            if (integer == null) {
                return -1;
            } else {
                return integer.intValue();
            }
        }
    }
}

// End RankFunDef.java
