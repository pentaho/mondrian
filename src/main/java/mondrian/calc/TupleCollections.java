/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.calc;

import mondrian.calc.impl.*;
import mondrian.olap.*;

import java.util.*;

/**
* Utility methods for tuple collections and iterators.
 *
 * @see TupleList
 * @see TupleIterator
 *
 * @author jhyde
 */
public final class TupleCollections {
    private static final TupleList[] EMPTY_LISTS = {
        new DelegatingTupleList(0, Collections.<List<Member>>emptyList()),
        new UnaryTupleList(Collections.<Member>emptyList()),
        new DelegatingTupleList(2, Collections.<List<Member>>emptyList()),
        new DelegatingTupleList(3, Collections.<List<Member>>emptyList()),
        new DelegatingTupleList(4, Collections.<List<Member>>emptyList())
    };

    // prevent instantiation
    private TupleCollections() {
    }

    /**
     * Creates a list of given arity.
     *
     * <p>If arity == 1, creates a {@link UnaryTupleList};
     * if arity == 0, creates a {@link DelegatingTupleList};
     * otherwise creates a {@link ArrayTupleList}.
     *
     * @see TupleList#cloneList(int)
     * @see #createList(int, int)
     *
     * @param arity Arity
     * @return Tuple list
     */
    public static TupleList createList(int arity) {
        switch (arity) {
        case 0:
            return new DelegatingTupleList(0, new ArrayList<List<Member>>());
        case 1:
            return new UnaryTupleList();
        default:
            return new ArrayTupleList(arity);
        }
    }

    /**
     * Creates a list of given arity and initial capacity.
     *
     * <p>If arity == 1, creates a {@link UnaryTupleList};
     * if arity == 0, creates a {@link DelegatingTupleList};
     * otherwise creates a {@link ArrayTupleList}.
     *
     * @see TupleList#cloneList(int)
     *
     * @param arity Arity
     * @param initialCapacity Initial capacity
     * @return Tuple list
     */
    public static TupleList createList(int arity, int initialCapacity) {
        switch (arity) {
        case 0:
            return new DelegatingTupleList(
                0, new ArrayList<List<Member>>(initialCapacity));
        case 1:
            return new UnaryTupleList(new ArrayList<Member>(initialCapacity));
        default:
            return new ArrayTupleList(arity, initialCapacity);
        }
    }

    /**
     * Returns an empty TupleList of given arity.
     *
     * @param arity Number of members per tuple
     * @return Empty tuple list
     */
    public static TupleList emptyList(int arity) {
        return arity < EMPTY_LISTS.length
            ? EMPTY_LISTS[arity]
            : new DelegatingTupleList(
                arity, Collections.<List<Member>>emptyList());
    }

    /**
     * Creates an unmodifiable TupleList backed by a given list.
     *
     * @see Collections#unmodifiableList(java.util.List)
     *
     * @param  list the list for which an unmodifiable view is to be returned.
     * @return an unmodifiable view of the specified list.
     */
    public static TupleList unmodifiableList(TupleList list) {
        return list.getArity() == 1
            ? new UnaryTupleList(
                Collections.unmodifiableList(
                    list.slice(0)))
            : new DelegatingTupleList(
                list.getArity(),
                Collections.unmodifiableList(
                    list));
    }

    /**
     * Adapts a {@link TupleCursor} into a {@link TupleIterator}.
     *
     * <p>Since the latter is a more difficult API to implement, the wrapper
     * has some extra state.
     *
     * <p>This method may be used to implement
     * {@link mondrian.calc.TupleIterable#tupleIterator()} for a
     * {@link TupleIterable} or {@link TupleList} that only has a
     * {@code TupleCursor} implementation.
     *
     * @param cursor Cursor
     * @return Tuple iterator view onto the cursor
     */
    public static TupleIterator iterator(final TupleCursor cursor) {
        if (cursor instanceof TupleIterator) {
            return (TupleIterator) cursor;
        }
        return new AbstractTupleIterator(cursor.getArity()) {
            private int state = STATE_UNKNOWN;

            private static final int STATE_UNKNOWN = 0;
            private static final int STATE_HASNEXT = 1;
            private static final int STATE_EOD = 2;

            public List<Member> current() {
                return cursor.current();
            }

            @Override
            public boolean hasNext() {
                switch (state) {
                case STATE_UNKNOWN:
                    if (cursor.forward()) {
                        state = STATE_HASNEXT;
                        return true;
                    } else {
                        state = STATE_EOD;
                        return false;
                    }
                case STATE_EOD:
                    return false;
                case STATE_HASNEXT:
                    return true;
                default:
                    throw new RuntimeException("unpexected state " + state);
                }
            }

            @Override
            public List<Member> next() {
                switch (state) {
                case STATE_UNKNOWN:
                    if (cursor.forward()) {
                        return cursor.current();
                    }
                    state = STATE_EOD;
                    // fall through
                case STATE_EOD:
                    throw new NoSuchElementException();
                case STATE_HASNEXT:
                    state = STATE_UNKNOWN;
                    return cursor.current();
                default:
                    throw new RuntimeException("unpexected state " + state);
                }
            }

            public boolean forward() {
                switch (state) {
                case STATE_UNKNOWN:
                    return cursor.forward();
                case STATE_HASNEXT:
                    state = STATE_UNKNOWN;
                    return true;
                case STATE_EOD:
                    return false;
                default:
                    throw new RuntimeException("unpexected state " + state);
                }
            }

            @Override
            public void setContext(Evaluator evaluator) {
                cursor.setContext(evaluator);
            }

            @Override
            public void currentToArray(Member[] members, int offset) {
                cursor.currentToArray(members, offset);
            }

            @Override
            public Member member(int column) {
                return cursor.member(column);
            }
        };
    }

    /**
     * Creates a slice of a {@link TupleIterable}.
     *
     * <p>Can be used as an implementation for
     * {@link mondrian.calc.TupleList#slice(int)}.
     *
     * @param tupleIterable Iterable
     * @param column Which member of each tuple of project.
     * @return Iterable that returns a given member of each tuple
     */
    public static Iterable<Member> slice(
        final TupleIterable tupleIterable,
        final int column)
    {
        if (column < 0 || column >= tupleIterable.getArity()) {
            throw new IllegalArgumentException();
        }
        return new Iterable<Member>() {
            public Iterator<Member> iterator() {
                final TupleIterator tupleIterator =
                    tupleIterable.tupleIterator();
                return new Iterator<Member>() {
                    public boolean hasNext() {
                        return tupleIterator.hasNext();
                    }

                    public Member next() {
                        if (!tupleIterator.forward()) {
                            throw new NoSuchElementException();
                        }
                        return tupleIterator.member(column);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Converts a {@link mondrian.calc.TupleIterable} to an old-style iterable that
     * creates an iterator over member arrays.
     *
     * @param tupleIterable Tuple iterable
     * @return Iterable that creates an iterator over tuples represented as
     *   member arrays
     */
    public static Iterable<Member[]> asMemberArrayIterable(
        final TupleIterable tupleIterable)
    {
        return new Iterable<Member[]>() {
            public Iterator<Member[]> iterator() {
                return new Iterator<Member[]>() {
                    final TupleIterator
                        tupleIterator = tupleIterable.tupleIterator();
                    public boolean hasNext() {
                        return tupleIterator.hasNext();
                    }

                    public Member[] next() {
                        final List<Member> next = tupleIterator.next();
                        return next.toArray(new Member[next.size()]);
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    /**
     * Converts a {@link mondrian.calc.TupleList} to an old-style list of member
     * arrays.
     *
     * @param tupleList Tuple list
     * @return List of member arrays
     */
    public static List<Member[]> asMemberArrayList(
        final TupleList tupleList)
    {
        return new AbstractList<Member[]>() {
            @Override
            public Member[] get(int index) {
                final List<Member> tuple = tupleList.get(index);
                return tuple.toArray(new Member[tuple.size()]);
            }

            @Override
            public int size() {
                return tupleList.size();
            }
        };
    }

    /**
     * Converts an old-style list (members or member arrays) to a
     * {@link mondrian.calc.TupleList}.
     *
     * <p>Deduces the arity of the list from the first element, if the list
     * is not empty. Otherwise assumes arity 1.
     *
     * <p>If the list happens to be a tuple list, returns unchanged.
     *
     * @param list Old-style list
     * @return Tuple list
     */
    public static TupleList asTupleList(List list) {
        if (list instanceof TupleList) {
            return (TupleList) list;
        }
        if (list.isEmpty()) {
            return TupleCollections.emptyList(1);
        }
        final Object o = list.get(0);
        if (o instanceof Member) {
            final List<Member> memberList = Util.cast(list);
            return new UnaryTupleList(memberList);
        } else {
            final List<Member[]> memberArrayList = Util.cast(list);
            return new DelegatingTupleList(
                memberArrayList.get(0).length,
                new AbstractList<List<Member>>() {
                    public List<Member> get(int index) {
                        return Arrays.asList(memberArrayList.get(index));
                    }

                    public int size() {
                        return memberArrayList.size();
                    }
                }
            );
        }
    }

    /**
     * Converts a {@link TupleIterable} into a {@link TupleList}.
     *
     * <p>If the iterable is already a list, returns the iterable. If it is not
     * a list, the behavior depends on the {@code eager} parameter. With eager =
     * true, creates a list and populates it with the contents of the
     * iterable. With eager = false, wraps in an adapter that implements the
     * list interface and materializes to a list the first time that an
     * operation that is in TupleList but not TupleIterable -- for example,
     * {@link TupleList#get} or {@link TupleList#size} -- is called.
     *
     * @param tupleIterable Iterable
     * @param eager Whether to convert into a list now, as opposed to on first
     *   use of a random-access method such as size or get.
     * @return List
     */
    public static TupleList materialize(
        TupleIterable tupleIterable,
        boolean eager)
    {
        if (tupleIterable instanceof TupleList) {
            return (TupleList) tupleIterable;
        }
        if (eager) {
            TupleList tupleList = createList(tupleIterable.getArity());
            TupleCursor tupleCursor = tupleIterable.tupleCursor();
            while (tupleCursor.forward()) {
                tupleList.addCurrent(tupleCursor);
            }
            return tupleList;
        } else {
            return new MaterializingTupleList(tupleIterable);
        }
    }

    /**
     * Implementation of {@link TupleList} that is based on a
     * {@link TupleIterable} and materializes into a read-only list the first
     * time that an method is called that requires a list.
     */
    private static class MaterializingTupleList
        implements TupleList
    {
        private final TupleIterable tupleIterable;
        TupleList tupleList;

        public MaterializingTupleList(
            TupleIterable tupleIterable)
        {
            this.tupleIterable = tupleIterable;
        }

        private TupleList materialize() {
            if (tupleList == null) {
                tupleList = TupleCollections.materialize(tupleIterable, true);
            }
            return tupleList;
        }

        // TupleIterable methods

        public TupleIterator tupleIterator() {
            if (tupleList == null) {
                return tupleIterable.tupleIterator();
            } else {
                return tupleList.tupleIterator();
            }
        }

        public TupleCursor tupleCursor() {
            if (tupleList == null) {
                return tupleIterable.tupleCursor();
            } else {
                return tupleList.tupleCursor();
            }
        }

        public int getArity() {
            return tupleIterable.getArity();
        }

        public Iterator<List<Member>> iterator() {
            if (tupleList == null) {
                return tupleIterable.iterator();
            } else {
                return tupleList.iterator();
            }
        }

        public List<Member> slice(int column) {
            // Note that TupleIterable has 'Iterable<Member> slice(int)'
            // and TupleList has 'List<Member> slice(int)'.
            // So, if this list is not materialized, we could return a slice of
            // the un-materialized iterable. But it's not worth the complexity.
            return materialize().slice(column);
        }

        public Member get(int slice, int index) {
            return materialize().get(slice, index);
        }

        public TupleList cloneList(int capacity) {
            return materialize().cloneList(capacity);
        }

        public void addTuple(Member... members) {
            throw new UnsupportedOperationException();
        }

        public TupleList project(int[] destIndices) {
            return materialize().project(destIndices);
        }

        public void addCurrent(TupleCursor tupleIter) {
            materialize().addCurrent(tupleIter);
        }

        public TupleList subList(int fromIndex, int toIndex) {
            return materialize().subList(fromIndex, toIndex);
        }

        public TupleList withPositionCallback(
            PositionCallback positionCallback)
        {
            return materialize().withPositionCallback(positionCallback);
        }

        public TupleList fix() {
            return materialize().fix();
        }

        public int size() {
            return materialize().size();
        }

        public boolean isEmpty() {
            return materialize().isEmpty();
        }

        public boolean contains(Object o) {
            return materialize().contains(o);
        }

        public Object[] toArray() {
            return materialize().toArray();
        }

        public <T> T[] toArray(T[] a) {
            return materialize().toArray(a);
        }

        public boolean add(List<Member> members) {
            throw new UnsupportedOperationException();
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean containsAll(Collection<?> c) {
            return materialize().containsAll(c);
        }

        public boolean addAll(Collection<? extends List<Member>> c) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(int index, Collection<? extends List<Member>> c) {
            throw new UnsupportedOperationException();
        }

        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public List<Member> get(int index) {
            return materialize().get(index);
        }

        public List<Member> set(int index, List<Member> element) {
            throw new UnsupportedOperationException();
        }

        public void add(int index, List<Member> element) {
            throw new UnsupportedOperationException();
        }

        public List<Member> remove(int index) {
            throw new UnsupportedOperationException();
        }

        public int indexOf(Object o) {
            return materialize().indexOf(o);
        }

        public int lastIndexOf(Object o) {
            return materialize().lastIndexOf(o);
        }

        public ListIterator<List<Member>> listIterator() {
            return materialize().listIterator();
        }

        public ListIterator<List<Member>> listIterator(int index) {
            return materialize().listIterator(index);
        }
    }
}

// End TupleCollections.java
