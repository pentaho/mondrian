/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2008-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.olap.type.SetType;
import mondrian.calc.Calc;
import mondrian.calc.ResultStyle;

import java.util.*;

/**
 * Evaluation context for a particular named set.
 *
 * @author jhyde
 * @since November 11, 2008
 * @version $Id$
 */
class RolapNamedSetEvaluator
    implements Evaluator.NamedSetEvaluator
{
    private final RolapResult.RolapResultEvaluatorRoot rrer;
    private final NamedSet namedSet;

    /** Value of this named set; set on first use. */
    private List list;

    /**
     * Dummy list used as a marker to detect re-entrant calls to
     * {@link #ensureList}.
     */
    private static final List DUMMY_LIST =
        Collections.unmodifiableList(Arrays.asList(new Object()));

    /**
     * Ordinal of current iteration through the named set. Used to implement
     * the &lt;Named Set&gt;.CurrentOrdinal and &lt;Named Set&gt;.Current
     * functions.
     */
    private int currentOrdinal;

    /**
     * Creates a RolapNamedSetEvaluator.
     *
     * @param rrer Evaluation root context
     * @param namedSet Named set
     */
    public RolapNamedSetEvaluator(
        RolapResult.RolapResultEvaluatorRoot rrer,
        NamedSet namedSet)
    {
        this.rrer = rrer;
        this.namedSet = namedSet;
    }

    public Iterable<Member> evaluateMemberIterable() {
        ensureList();
        return new IterableCollection<Member>();
    }

    public Iterable<Member[]> evaluateTupleIterable() {
        ensureList();
        return new IterableCollection<Member[]>();
    }

    /**
     * Evaluates and saves the value of this named set, if it has not been
     * evaluated already.
     *
     * @param <T> Type of member of this set: Member or Member[].
     */
    private <T> void ensureList() {
        if (list != null) {
            if (list == DUMMY_LIST) {
                throw rrer.slicerEvaluator.newEvalException(
                    null,
                    "Illegal attempt to reference value of named set '"
                    + namedSet.getName() + "' while evaluating itself");
            }
            return;
        }
        if (RolapResult.LOGGER.isDebugEnabled()) {
            RolapResult.LOGGER.debug(
                "Named set " + namedSet.getName() + ": starting evaluation");
        }
        list = DUMMY_LIST; // recursion detection
        final RolapEvaluatorRoot root =
            rrer.slicerEvaluator.root;
        final Calc calc =
            root.getCompiled(namedSet.getExp(), false, ResultStyle.ITERABLE);
        Object o =
            rrer.result.evaluateExp(
                calc,
                rrer.slicerEvaluator.push());
        final List<T> rawList;

        // Axes can be in two forms: list or iterable. If iterable, we
        // need to materialize it, to ensure that all cell values are in
        // cache.
        if (o instanceof List) {
            //noinspection unchecked
            rawList = (List<T>) o;
        } else {
            Iterable<T> iter = Util.castToIterable(o);
            rawList = new ArrayList<T>();
            for (T e : iter) {
                rawList.add(e);
            }
        }
        if (RolapResult.LOGGER.isDebugEnabled()) {
            final StringBuilder buf = new StringBuilder();
            buf.append(this);
            buf.append(": ");
            buf.append("Named set ");
            buf.append(namedSet.getName());
            buf.append(" evaluated to:");
            buf.append(Util.nl);
            int arity = ((SetType) calc.getType()).getArity();
            int rowCount = 0;
            final int maxRowCount = 100;
            if (arity == 1) {
                for (Member t : Util.<Member>cast(rawList)) {
                    if (rowCount++ > maxRowCount) {
                        buf.append("...");
                        buf.append(Util.nl);
                        break;
                    }
                    buf.append(t);
                    buf.append(Util.nl);
                }
            } else {
                for (Member[] t : Util.<Member[]>cast(rawList)) {
                    if (rowCount++ > maxRowCount) {
                        buf.append("...");
                        buf.append(Util.nl);
                        break;
                    }
                    int k = 0;
                    for (Member member : t) {
                        if (k++ > 0) {
                            buf.append(", ");
                        }
                        buf.append(member);
                    }
                    buf.append(Util.nl);
                }
            }
            RolapResult.LOGGER.debug(buf);
        }
        // Wrap list so that currentOrdinal is updated whenever the list
        // is accessed. The list is immutable, because we don't override
        // AbstractList.set(int, Object).
        this.list = new AbstractList<T>() {
            public T get(int index) {
                currentOrdinal = index;
                return rawList.get(index);
            }

            public int size() {
                return rawList.size();
            }
        };
    }

    public int currentOrdinal() {
        return currentOrdinal;
    }

    public Member[] currentTuple() {
        return (Member[]) list.get(currentOrdinal);
    }

    public Member currentMember() {
        return (Member) list.get(currentOrdinal);
    }

    /**
     * Collection that implements only methods {@code iterator}, {@code size},
     * {@code isEmpty}.
     *
     * <p>Implements {@link Iterable} explicitly because Collection does not
     * implement Iterable until JDK1.5. This way, we don't have to use a
     * wrapper that hides the size method.
     *
     * @param <T> Element type
     */
    private class IterableCollection<T>
            implements Collection<T>, Iterable<T>, List<T>
    {
        public Iterator<T> iterator() {
            return listIterator();
        }

        // The following are included to fill out the Collection
        // interface, but anything that would alter the collection
        // or is not strictly needed elsewhere is unsupported
        public boolean add(T o) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public boolean contains(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public boolean isEmpty() {
            return list.isEmpty();
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public int size() {
            return list.size();
        }

        public Object[] toArray() {
            return list.toArray();
        }

        public <T> T[] toArray(T[] a) {
            return (T[]) list.toArray(a);
        }

        // the rest fill out the List interface, again mostly unsupported
        public void add(int index, T element) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(int index, Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        public T get(int index) {
            return (T)list.get(index);
        }

        public int indexOf(Object o) {
            return list.indexOf(o);
        }

        public int lastIndexOf(Object o) {
            return list.lastIndexOf(o);
        }

        public ListIterator<T> listIterator() {
            return new ListIterator<T>() {
                int i = -1;

                public boolean hasNext() {
                    return i < list.size() - 1;
                }

                public boolean hasPrevious() {
                    return i > 0;
                }

                public T next() {
                    currentOrdinal = ++i;
                    //noinspection unchecked
                    return (T) list.get(i);
                }

                public T previous() {
                    currentOrdinal = --i;
                    return (T)list.get(i);
                }

                public int nextIndex() {
                    return i + 1;
                }

                public int previousIndex() {
                    return i;
                }

                public void set(T o) {
                    throw new UnsupportedOperationException();
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }

                public void add(T o) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        public ListIterator<T> listIterator(int index) {
            // TODO: should actually implement a bidirectional
            //       iterator here, have listIterator() call
            //       this, and have iterator() call listIterator().
            //       for now, nothing seems to call listIterator(),
            //       so this works, and saves a copy from iterable
            //       to list higher up in the call stack
            throw new UnsupportedOperationException();
        }

        public T remove(int index) {
            throw new UnsupportedOperationException();
        }

        public T set(int index, T element) {
            return (T)list.set(index, element);
        }

        public List<T> subList(int fromIndex, int toIndex) {
            return list.subList(fromIndex, toIndex);
        }
    }
}

// End RolapNamedSetEvaluator.java
