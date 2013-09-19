/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 Julian Hyde
// Copyright (C) 2005-2011 Pentaho and others
// All Rights Reserved.
*/

package mondrian.olap.fun;

import mondrian.calc.*;
import mondrian.calc.impl.AbstractListCalc;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.*;

import java.util.*;

/**
 * Definition of the <code>INTERSECT</code> MDX function.
 *
 * @author jhyde
 * @since Mar 23, 2006
 */
class IntersectFunDef extends FunDefBase
{
    private static final String[] ReservedWords = new String[] {"ALL"};

    static final Resolver resolver =
        new ReflectiveMultiResolver(
            "Intersect",
            "Intersect(<Set1>, <Set2>[, ALL])",
            "Returns the intersection of two input sets, optionally retaining duplicates.",
            new String[] {"fxxxy", "fxxx"},
            IntersectFunDef.class,
            ReservedWords);

    public IntersectFunDef(FunDef dummyFunDef)
    {
        super(dummyFunDef);
    }

    public Calc compileCall(ResolvedFunCall call, ExpCompiler compiler) {
        final String literalArg = getLiteralArg(call, 2, "", ReservedWords);
        final boolean all = literalArg.equalsIgnoreCase("ALL");
        final int arity = call.getType().getArity();

        final ListCalc listCalc1 = compiler.compileList(call.getArg(0));
        final ListCalc listCalc2 = compiler.compileList(call.getArg(1));
        return new AbstractListCalc(
            call, new Calc[] {listCalc1, listCalc2})
        {
            public TupleList evaluateList(Evaluator evaluator) {
                TupleList leftList =
                    listCalc1.evaluateList(evaluator);
                if (leftList.isEmpty()) {
                    return leftList;
                }
                final TupleList rightList =
                    listCalc2.evaluateList(evaluator);
                if (rightList.isEmpty()) {
                    return rightList;
                }

                // Set of members from the right side of the intersect.
                // We use a RetrievableSet because distinct keys
                // (regular members and visual totals members) compare
                // identical using hashCode and equals, we want to retrieve
                // the actual key, and java.util.Set only has containsKey.
                RetrievableSet<List<Member>> rightSet =
                    new RetrievableHashSet<List<Member>>(
                        rightList.size() * 3 / 2);
                for (List<Member> tuple : rightList) {
                    rightSet.add(tuple);
                }

                final TupleList result =
                    TupleCollections.createList(
                        arity, Math.min(leftList.size(), rightList.size()));
                final Set<List<Member>> resultSet =
                    all ? null : new HashSet<List<Member>>();
                for (List<Member> leftTuple : leftList) {
                    List<Member> rightKey = rightSet.getKey(leftTuple);
                    if (rightKey == null) {
                        continue;
                    }
                    if (resultSet != null && !resultSet.add(leftTuple)) {
                        continue;
                    }
                    result.add(
                        copyTupleWithVisualTotalsMembersOverriding(
                            leftTuple, rightKey));
                }
                return result;
            }

            /**
             * Constructs a tuple consisting of members from
             * {@code leftTuple}, but overridden by any corresponding
             * members from {@code rightKey} that happen to be visual totals
             * members.
             *
             * <p>Returns the original tuple if there are no visual totals
             * members on the RHS.
             *
             * @param leftTuple Original tuple
             * @param rightKey Right tuple
             * @return Copy of original tuple, with any VisualTotalMembers
             *   from right tuple overriding
             */
            private List<Member> copyTupleWithVisualTotalsMembersOverriding(
                List<Member> leftTuple, List<Member> rightKey)
            {
                List<Member> tuple = leftTuple;
                for (int i = 0; i < rightKey.size(); i++) {
                    Member member = rightKey.get(i);
                    if (!(tuple.get(i)
                        instanceof VisualTotalsFunDef.VisualTotalMember)
                        && member instanceof
                        VisualTotalsFunDef.VisualTotalMember)
                    {
                        if (tuple == leftTuple) {
                            // clone on first VisualTotalMember -- to avoid
                            // alloc/copy in the common case where there are
                            // no VisualTotalMembers
                            tuple = new ArrayList<Member>(leftTuple);
                        }
                        tuple.set(i, member);
                    }
                }
                return tuple;
            }
        };
    }

    /**
     * Interface similar to the Set interface that allows key values to be
     * returned.
     *
     * <p>Useful if multiple objects can compare equal (using
     * {@link Object#equals(Object)} and {@link Object#hashCode()}, per the
     * set contract) and you wish to distinguish them after they have been added
     * to the set.
     *
     * @param <E> element type
     */
    private interface RetrievableSet<E> {
        /**
         * Returns the key in this set that compares equal to a given object,
         * or null if there is no such key.
         *
         * @param e Key value
         * @return Key in the set equal to given key value
         */
        E getKey(E e);

        /**
         * Analogous to {@link Set#add(Object)}.
         *
         * @param e element to be added to this set
         * @return <tt>true</tt> if this set did not already contain the
         *         specified element
         */
        boolean add(E e);
    }

    private static class RetrievableHashSet<E>
        extends HashMap<E, E>
        implements RetrievableSet<E>
    {
        public RetrievableHashSet(int initialCapacity) {
            super(initialCapacity);
        }

        public E getKey(E e) {
            return super.get(e);
        }

        public boolean add(E e) {
            return super.put(e, e) == null;
        }
    }
}

// End IntersectFunDef.java
