/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2011 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.calc.*;
import mondrian.olap.Axis;
import mondrian.olap.Member;
import mondrian.olap.Position;
import mondrian.util.UnsupportedList;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.ListIterator;
import java.util.Iterator;
import java.util.List;

/**
 * Derived classes of RolapAxis implements the Axis interface which are
 * specializations based upon the number of Positions, how each Position's
 * Members are orgainized and whether the Members/Member[]s are in a List
 * or an Iterable.
 *
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 */
public abstract class RolapAxis implements Axis {
    private static final Logger LOGGER = Logger.getLogger(RolapAxis.class);

    public static String toString(Axis axis) {
        List<Position> pl = axis.getPositions();
        return toString(pl);
    }

    public static String toString(List<Position> pl) {
        StringBuilder buf = new StringBuilder();
        for (Position p : pl) {
            buf.append('{');
            boolean firstTime = true;
            for (Member m : p) {
                if (! firstTime) {
                    buf.append(", ");
                }
                buf.append(m.getUniqueName());
                firstTime = false;
            }
            buf.append('}');
            buf.append('\n');
        }
        return buf.toString();
    }

    /**
     * A Wrapper has many uses. In particular, if one is using Java 5 or
     * above, one can create a Wrapper that is also a memory usage listener.
     * Then one can place an Axis implementation into a Wrapper where the
     * initial implementation is in-memory, large-memory-usage and
     * cpu fast. The on the first memory notification it can be migrated
     * to an in-memory, small-memory-usage and cpu slower. On a subsequent
     * memory notification it can be migrated to an on-disk, low-memory and
     * cpu slow implementation.
     */
    public static class Wrapper extends RolapAxis {
        private final Axis axis;
        protected Wrapper(Axis axis) {
            super();
            this.axis = axis;
        }
        public List<Position> getPositions() {
            return this.axis.getPositions();
        }
    }

    /**
     * The NoPosition Axis implementation is an Axis that has no Positions,
     * the size of the list of positions is zero.
     */
    public static class NoPosition extends RolapAxis {
        public NoPosition() {
            super();
        }

        public List<Position> getPositions() {
            return Collections.emptyList();
        }
    }

    /**
     * The PositionList Axis implementation takes a List of positions.
     */
    public static class PositionList extends RolapAxis {
        protected final List<Position> positions;
        public PositionList(List<Position> positions) {
            super();
            this.positions = positions;
        }
        public List<Position> getPositions() {
            return positions;
        }
    }

    /**
     * A SingleEmptyPosition has a single Position and the Position has
     * no Members.
     */
    public static class SingleEmptyPosition extends RolapAxis {
        private static final Position EMPTY_POSITION =
            (Position) new EmptyPosition();
        private static final List<Position> EMPTY_POSITIONS =
            Collections.singletonList(EMPTY_POSITION);

        public SingleEmptyPosition() {
        }

        public List<Position> getPositions() {
            return EMPTY_POSITIONS;
        }

        static class EmptyPosition extends PositionBase {
            EmptyPosition() {
            }

            @Override
            protected List<Member> getList() {
                return Collections.emptyList();
            }
        }
    }

    /**
     * A MemberArrayIterable takes an Iterable&lt;Member[]&gt; where
     * each Position has
     * an array of Members from the corresponding location in the iterator.
     * If the client request any of the List, non-Iterable, API, then
     * a List is materialized from the Iterable.
     */
    public static class MemberArrayIterable extends RolapAxis {
        private TupleIterable iterable;
        private TupleList list;

        public MemberArrayIterable(TupleIterable iterable) {
            this.iterable = iterable;
            this.list = null;
        }

        public synchronized List<Position> getPositions() {
            return (list == null)
                ? new MemberArrayIterable.PositionWrapper()
                : new MemberArrayIterable.PositionList();
        }

        protected synchronized void materialize() {
            if (list == null) {
                list = TupleCollections.createList(iterable.getArity());
                final TupleCursor cursor = iterable.tupleCursor();
                while (cursor.forward()) {
                    list.addCurrent(cursor);
                }
                // allow gc of iter
                iterable = null;
            }
        }

        /**
         * This List&lt;Position&gt; starts life with a List&lt;Position&gt;
         * implementation
         * that is based upon an non-List (Iterable). If all accesses
         * are simply through iteration, then the initial implementation
         * remains, but if the client uses either the 'size' or 'get' methods
         * then the Iterable is materialized into a List.
         */
        class PositionWrapper extends PositionListUnsupported {
            List<Position> positionList;
            PositionWrapper() {
                positionList = new PositionIter();
            }

            protected synchronized void materialize() {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "PositionWrapper.materialize: Member[] iter.class="
                        + ((iterable != null)
                            ? iterable.getClass().getName()
                            : null));
                }
                RolapAxis.MemberArrayIterable.this.materialize();
                positionList = new MemberArrayIterable.PositionList();
            }

            public int size() {
                try {
                    return positionList.size();
                } catch (UnsupportedOperationException ex) {
                    this.materialize();
                    return positionList.size();
                }
            }

            public Position get(int index) {
                try {
                    return positionList.get(index);
                } catch (UnsupportedOperationException ex) {
                    this.materialize();
                    return positionList.get(index);
                }
            }

            public Iterator<Position> iterator() {
                return positionList.iterator();
            }
        }

        /**
         * PositionIter is a List&lt;Position&gt; that only support the
         * 'iterator' method. This assumes that one iterates over Positions
         * and for each Postion one iterates over Members. Each Position
         * has two or more Members.
         */
        class PositionIter extends PositionIterBase {
            private TupleIterator it;

            PositionIter() {
                it = iterable.tupleIterator();
            }

            public Iterator<Position> iterator() {
                return new Iterator<Position>() {
                    int nextCnt = 0;

                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    public Position next() {
                        nextCnt++;
                        return new MemberIterPosition(it.next());
                    }

                    public void remove() {
                        throw new UnsupportedOperationException("remove");
                    }
                };
            }
        }

        /**
         * A List&lt;Member&gt; which only implements the 'iterator' method.
         * Each Iterator&lt;Member&gt; two or more Members.
         */
        class MemberIterPosition extends PositionBase {
            final List<Member> members;

            MemberIterPosition(List<Member> members) {
                this.members = members;
            }

            @Override
            protected List<Member> getList() {
                return members;
            }

            public Iterator<Member> iterator() {
                return members.iterator();
            }
        }

        /**
         *  Each Position has two or more Members.
         */
        class PositionList extends PositionListBase {
            PositionList() {
            }

            public boolean isEmpty() {
                // may be considerably cheaper than computing size
                return list.isEmpty();
            }

            public int size() {
                return list.size();
            }

            public Position get(int index) {
                return new MemberArrayListPosition(index);
            }
        }

        /**
         * Allows access only the the Member at the given offset.
         */
        class MemberArrayListPosition extends PositionBase {
            protected final int offset;

            MemberArrayListPosition(int offset) {
                this.offset = offset;
            }

            @Override
            protected List<Member> getList() {
                return list.get(offset);
            }
        }
    }

    /**
     * A MemberArrayList takes a List&lt;Member[]&gt; where each Position has
     * the Member's from the corresponding location in the list.
     * It is assumed that each element of the list has an array of Members of
     * the same size.
     */
    public static class MemberArrayList extends RolapAxis {
        private final TupleList list;

        public MemberArrayList(TupleList list) {
            this.list = list;
        }

        public List<Position> getPositions() {
            return new MemberArrayList.PositionList();
        }

        /**
         *  Each Position has an array of Member.
         */
        class PositionList extends PositionListBase {
            PositionList() {
            }

            public int size() {
                return list.size();
            }

            public boolean isEmpty() {
                // may be considerably cheaper than computing size
                return list.isEmpty();
            }

            public Position get(int index) {
                if (index >= list.size()) {
                    throw new IndexOutOfBoundsException();
                }
                return new MemberArrayListPosition(index);
            }
        }

        /**
         *  Allows access only the the Member at the given offset plus index.
         */
        class MemberArrayListPosition extends PositionBase {
            protected final int offset;

            MemberArrayListPosition(int offset) {
                this.offset = offset;
            }

            @Override
            protected List<Member> getList() {
                return list.get(offset);
            }
        }
    }

    /**
     * A List&lt;Member&gt; for which all methods throw the
     * UnsupportedOperationException exception when invoked. Derived classes
     * can implement those methods that they require.
     */
    protected static abstract class PositionUnsupported
        extends UnsupportedList<Member>
        implements Position
    {
        protected PositionUnsupported() {
        }
    }

    /**
     * The PositionBase is an abstract implementation of the Position
     * interface and provides both Iterator&lt;Member&gt; and
     * ListIterator&lt;Member&gt; implementations.
     */
    protected static abstract class PositionBase
        extends PositionUnsupported
    {
        protected PositionBase() {
        }

        protected abstract List<Member> getList();

        @Override
        public int size() {
            return getList().size();
        }

        @Override
        public Member get(int index) {
            return getList().get(index);
        }

        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof List)) {
                return false;
            }
            List that = (List) o;
            final int size = this.size();
            if (size != that.size()) {
                return false;
            }
            for (int i = 0; i < size; i++) {
                final Member m1 = get(i);
                final Object m2 = that.get(i);
                if (!(m1 == null
                    ? m2 == null
                    : m1.equals(m2)))
                {
                    return false;
                }
            }
            return true;
        }

        public int hashCode() {
            int hashCode = 1;
            int size = size();
            for (int i = 0; i < size; i++) {
                Member obj = get(i);
                hashCode =
                    31 * hashCode + (obj == null
                        ? 0
                        : obj.hashCode());
            }
            return hashCode;
        }

        @Override
        public String toString() {
            return getList().toString();
        }

        public ListIterator<Member> listIterator() {
            return new ListItr(0);
        }

        public ListIterator<Member> listIterator(int index) {
            return new ListItr(index);
        }

        public Iterator<Member> iterator() {
            return new Itr();
        }
    }

    protected static abstract class PositionListUnsupported
        extends UnsupportedList<Position>
    {
        protected PositionListUnsupported() {
        }
    }

    protected static abstract class PositionIterBase
        extends PositionListUnsupported
    {
        protected PositionIterBase() {
        }

        public abstract Iterator<Position> iterator();
    }

    /**
     * The PositionListBase is an abstract implementation of the
     * List&lt;Position&gt
     * interface and provides both Iterator&lt;Position&gt; and
     * ListIterator&lt;Position&gt; implementations.
     */
    protected static abstract class PositionListBase
        extends PositionListUnsupported
    {
        protected PositionListBase() {
            super();
        }

        public abstract int size();
        public abstract boolean isEmpty();
        public abstract Position get(int index);

        // Collection
        public ListIterator<Position> listIterator() {
            return new ListItr(0);
        }
        public ListIterator<Position> listIterator(int index) {
            return new ListItr(index);
        }
        public Iterator<Position> iterator() {
            return new Itr();
        }
    }

    protected RolapAxis() {
    }

    public abstract List<Position> getPositions();
}
// End RolapAxis.java
