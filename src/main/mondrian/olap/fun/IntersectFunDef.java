/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2004-2002 Kana Software, Inc.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.fun;

import mondrian.calc.impl.*;
import mondrian.olap.*;
import mondrian.calc.*;
import mondrian.mdx.ResolvedFunCall;
import mondrian.olap.type.SetType;

import java.util.*;

/**
 * Definition of the <code>INTERSECT</code> MDX function.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 23, 2006
 */
class IntersectFunDef extends FunDefBase
{
    private static final String[] ReservedWords = new String[] {"ALL"};

    static final Resolver resolver = new ReflectiveMultiResolver(
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
        final int arity = ((SetType) call.getType()).getArity();

        if (arity == 1) {
            final MemberListCalc listCalc1 =
                (MemberListCalc) compiler.compileList(call.getArg(0));
            final MemberListCalc listCalc2 =
                (MemberListCalc) compiler.compileList(call.getArg(1));
            return new AbstractMemberListCalc(
                call, new Calc[] {listCalc1, listCalc2})
            {
                public List<Member> evaluateMemberList(Evaluator evaluator) {
                    List<Member> leftList =
                        listCalc1.evaluateMemberList(evaluator);
                    if (leftList.isEmpty()) {
                        return Collections.emptyList();
                    }
                    List<Member> rightList =
                        listCalc2.evaluateMemberList(evaluator);
                    if (rightList.isEmpty()) {
                        return Collections.emptyList();
                    }
                    // Set of members from the right side of the intersect.
                    // We use a RetrievableSet because distinct keys
                    // (regular members and visual totals members) compare
                    // identical using hashCode and equals, we want to retrieve
                    // the actual key, and java.util.Set only has containsKey.
                    final RetrievableSet<Member> rightSet =
                        new RetrievableHashSet<Member>();
                    for (Member member : rightList) {
                        rightSet.add(member);
                    }
                    final List<Member> result = new ArrayList<Member>();
                    final Set<Member> resultSet =
                        all ? null : new HashSet<Member>();
                    for (Member leftMember : leftList) {
                        final Member rightMember = rightSet.getKey(leftMember);
                        if (rightMember == null) {
                            continue;
                        }
                        if (resultSet != null && !resultSet.add(leftMember)) {
                            continue;
                        }
                        if (rightMember
                            instanceof VisualTotalsFunDef.VisualTotalMember)
                        {
                            result.add(rightMember);
                        } else {
                            result.add(leftMember);
                        }
                    }
                    return result;
                }
            };
        } else {
            final TupleListCalc listCalc1 =
                (TupleListCalc) compiler.compileList(call.getArg(0));
            final TupleListCalc listCalc2 =
                (TupleListCalc) compiler.compileList(call.getArg(1));
            return new AbstractTupleListCalc(
                call, new Calc[] {listCalc1, listCalc2})
            {
                public List<Member[]> evaluateTupleList(Evaluator evaluator) {
                    List<Member[]> leftList =
                        listCalc1.evaluateTupleList(evaluator);
                    if (leftList.isEmpty()) {
                        return Collections.emptyList();
                    }
                    final List<Member[]> rightList =
                        listCalc2.evaluateTupleList(evaluator);
                    if (rightList.isEmpty()) {
                        return Collections.emptyList();
                    }
                    RetrievableSet<List<Member>> rightSet =
                        buildSearchableCollection(rightList);
                    final List<Member[]> result = new ArrayList<Member[]>();
                    final Set<List<Member>> resultSet =
                        all ? null : new HashSet<List<Member>>();
                    for (Member[] leftTuple : leftList) {
                        List<Member> leftKey = Arrays.asList(leftTuple);
                        List<Member> rightKey = rightSet.getKey(leftKey);
                        if (rightKey == null) {
                            continue;
                        }
                        if (resultSet != null && !resultSet.add(leftKey)) {
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
                private Member[] copyTupleWithVisualTotalsMembersOverriding(
                    Member[] leftTuple,
                    List<Member> rightKey)
                {
                    Member[] tuple = leftTuple;
                    for (int i = 0; i < rightKey.size(); i++) {
                        Member member = rightKey.get(i);
                        if (!(tuple[i]
                            instanceof VisualTotalsFunDef.VisualTotalMember)
                            && member instanceof
                            VisualTotalsFunDef.VisualTotalMember)
                        {
                            if (tuple == leftTuple) {
                                // clone on first VisualTotalMember -- to avoid
                                // alloc/copy in the common case where there are
                                // no VisualTotalMembers
                                tuple = leftTuple.clone();
                            }
                            tuple[i] = member;
                        }
                    }
                    return tuple;
                }

                private RetrievableSet<List<Member>> buildSearchableCollection(
                    List<Member[]> tuples)
                {
                    RetrievableSet<List<Member>> result =
                        new RetrievableHashSet<List<Member>>(tuples.size());
                    for (Member[] tuple : tuples) {
                        result.add(Arrays.asList(tuple));
                    }
                    return result;
                }
            };
        }
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

        public RetrievableHashSet() {
            super();
        }

        public E getKey(E e) {
            return super.get(e);
        }

        /*
        public boolean contains(Object o) {
            return super.containsKey(o);
        }

        public Iterator<E> iterator() {
            return super.keySet().iterator();
        }

        public Object[] toArray() {
            return super.keySet().toArray();
        }

        public <T> T[] toArray(T[] a) {
            return super.keySet().toArray(a);
        }
        */

        public boolean add(E e) {
            return super.put(e, e) == null;
        }

        /*
        public boolean containsAll(Collection<?> c) {
            return super.keySet().containsAll(c);
        }

        public boolean addAll(Collection<? extends E> c) {
            for (E e : c) {
                put(e, e);
            }
            return c.size() > 0;
        }

        public boolean retainAll(Collection<?> c) {
            return super.keySet().retainAll(c);
        }

        public boolean removeAll(Collection<?> c) {
            return super.keySet().removeAll(c);
        }
        */
    }
}

// End IntersectFunDef.java
