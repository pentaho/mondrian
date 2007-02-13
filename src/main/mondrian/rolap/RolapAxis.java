/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap;


import mondrian.olap.Axis;
import mondrian.olap.Member;
import mondrian.olap.Position;
import mondrian.util.UnsupportedList;
import org.apache.log4j.Logger;
import java.util.Collection;
import java.util.Collections;
import java.util.ListIterator;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

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
            return Collections.EMPTY_LIST;
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
        public SingleEmptyPosition() {
        }
        public List<Position> getPositions() {
            return Collections.singletonList((Position) new EmptyPosition());
        }
        static class EmptyPosition extends PositionBase {
            EmptyPosition() {
            }
            public int size() {
                return 0;
            }
            public Member get(int index) {
                throw new IndexOutOfBoundsException(
                        "Index: "+index+", Size: 0");
            }
        }
    }

    /**
     * A MemberIterable takes an Iterable&lt;Member&gt; where each Position has
     * a single Member from the corresponding location in the iterator.
     * If the client request any of the List, non-Iterable, API, then
     * a List is materialized from the Iterable.
     */
    public static class MemberIterable extends RolapAxis {
        private Iterable<Member> iter;
        private List<Member> list;
        public MemberIterable(Iterable<Member> iter) {
            this.iter = iter;
            this.list = null;
        }
        public synchronized List<Position> getPositions() {
            return (list == null)
                ? new PositionWrapper()
                : new PositionList();
        }
        protected synchronized void materialize() {
            if (list == null) {
                Iterator<Member> it = iter.iterator();
                list = new ArrayList<Member>();
                while (it.hasNext()) {
                    list.add(it.next());
                }
                // allow gc of iter
                iter = null;
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
//System.out.println("materialize Member");
//new Exception().printStackTrace();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                       "PositionWrapper.materialize: Member iter.class="
                             + iter.getClass().getName());
                }
                RolapAxis.MemberIterable.this.materialize();
                positionList = new PositionList();
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
         * and for each Postion one iterates over Members. In this case,
         * each Position has a single Member.
         */
        class PositionIter extends PositionIterBase {
            private Iterator<Member> it;
            PositionIter() {
                it = iter.iterator();
            }
            public Iterator<Position> iterator() {
                return new Iterator<Position>() {
                    public boolean hasNext() {
                        return it.hasNext();
                    }
                    public Position next() {
                        return new MIPosition(it.next());
                    }
                    public void remove() {
                        throw new UnsupportedOperationException("remove");
                    }
                };
            }
        }

        /**
         * A List&lt;Member&gt; which only implements the 'iterator' method.
         * Each Iterator&lt;Member&gt; has only one Member.
         */
        class MIPosition extends PositionUnsupported {
            Member member;
            MIPosition(Member member) {
                this.member = member;
            }

            public Iterator<Member> iterator() {
                return new Iterator<Member>() {
                    public boolean hasNext() {
                        return (member != null);
                    }
                    public Member next() {
                        try {
                            return member;
                        } finally {
                            member = null;
                        }
                    }
                    public void remove() {
                        throw new UnsupportedOperationException("remove");
                    }
                };
            }
        }

        /**
         *  Each Position has a single Member.
         */
        class PositionList extends PositionListBase {
            PositionList() {
            }
            public int size() {
                return list.size();
            }
            public Position get(int index) {
                return new MLPosition(index);
            }
        }

        /**
         *  Allows access only the the Member at the given offset.
         */
        class MLPosition extends PositionBase {
            protected final int offset;
            MLPosition(int offset) {
                this.offset = offset;
            }
            public int size() {
                return 1;
            }
            public Member get(int index) {
                if (index != 0) {
                    throw new IndexOutOfBoundsException(
                        "Index: "+index+", Size: 1");
                }
                return list.get(offset);
            }
        }
    }



    /**
     * A MemberList takes a List&lt;Member&gt; where each Position has
     * a single Member from the corresponding location in the list.
     */
    public static class MemberList extends RolapAxis {
        private final List<Member> list;
        public MemberList(List<Member> list) {
            this.list = list;
        }
        public List<Position> getPositions() {
            return new PositionList();
        }
        /**
         *  Each Position has a single Member.
         */
        class PositionList extends PositionListBase {
            PositionList() {
            }
            public int size() {
                return list.size();
            }
            public Position get(int index) {
                return new MLPosition(index);
            }
        }

        /**
         *  Allows access only the the Member at the given offset.
         */
        class MLPosition extends PositionBase {
            protected final int offset;
            MLPosition(int offset) {
                this.offset = offset;
            }
            public int size() {
                return 1;
            }
            public Member get(int index) {
                if (index != 0) {
                    throw new IndexOutOfBoundsException(
                        "Index: "+index+", Size: 1");
                }
                return list.get(offset);
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
        private Iterable<Member[]> iter;
        private List<Member[]> list;
        private int len;
        public MemberArrayIterable(Iterable<Member[]> iter) {
            this.iter = iter;
            this.list = null;
            this.len = 0;
        }
        public synchronized List<Position> getPositions() {
            return (list == null)
                ? new PositionWrapper()
                : new PositionList();
        }
        protected synchronized void materialize() {
            if (list == null) {
                Iterator<Member[]> it = iter.iterator();
                list = new ArrayList<Member[]>();
                while (it.hasNext()) {
                    list.add(it.next());
                }
                // allow gc of iter
                iter = null;

                len = (list.size() == 0) ? 0 : list.get(0).length;
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
//System.out.println("materialize Member[]");
//new Exception().printStackTrace();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                       "PositionWrapper.materialize: Member[] iter.class="
                             + iter.getClass().getName());
                }
                RolapAxis.MemberArrayIterable.this.materialize();
                positionList = new PositionList();
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
            private Iterator<Member[]> it;
            PositionIter() {
                it = iter.iterator();
            }
            public Iterator<Position> iterator() {
                return new Iterator<Position>() {
                    int nextCnt = 0;
                    public boolean hasNext() {
                        return it.hasNext();
                    }
                    public Position next() {
                        nextCnt++;
                        return new MIPosition(it.next());
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
        class MIPosition extends PositionUnsupported {
            Member[] members;
            MIPosition(Member[] members) {
                this.members = members;
            }

            public Iterator<Member> iterator() {
                return new Iterator<Member>() {
                    int index = 0;
                    public boolean hasNext() {
                        return (index < members.length);
                    }
                    public Member next() {
                        return members[index++];
                    }
                    public void remove() {
                        throw new UnsupportedOperationException("remove");
                    }
                };
            }
        }

        /**
         *  Each Position has two or more Members.
         */
        class PositionList extends PositionListBase {
            PositionList() {
            }
            public int size() {
                return list.size();
            }
            public Position get(int index) {
                return new MALPosition(index);
            }
        }

        /**
         *  Allows access only the the Member at the given offset.
         */
        class MALPosition extends PositionBase {
            protected final int offset;
            MALPosition(int offset) {
                this.offset = offset;
            }
            public int size() {
                return RolapAxis.MemberArrayIterable.this.len;
            }
            public Member get(int index) {
                if (index > RolapAxis.MemberArrayIterable.this.len) {
                    throw new IndexOutOfBoundsException(
                        "Index: " +
                        index +
                        ", Size: " +
                        RolapAxis.MemberArrayIterable.this.len);
                }
                return list.get(offset)[index];
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
        private final List<Member[]> list;
        private final int len;
        public MemberArrayList(List<Member[]> list) {
            this.list = list;
            this.len = (list.size() == 0) ? 0 : list.get(0).length;
        }
        public List<Position> getPositions() {
            return new PositionList();
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
            public Position get(int index) {
                return new MALPosition(index);
            }
        }
        /**
         *  Allows access only the the Member at the given offset plus index.
         */
        class MALPosition extends PositionBase {
            protected final int offset;
            MALPosition(int offset) {
                this.offset = offset;
            }
            public int size() {
                return RolapAxis.MemberArrayList.this.len;
            }
            public Member get(int index) {
                if (index > RolapAxis.MemberArrayList.this.len) {
                    throw new IndexOutOfBoundsException(
                        "Index: " +
                        index +
                        ", Size: " +
                        RolapAxis.MemberArrayList.this.len);
                }
                return list.get(offset)[index];
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
                        implements Position {
        protected PositionUnsupported() {
        }
    }
    /**
     * The PositionBase is an abstract implementation of the Position
     * interface and provides both Iterator&lt;Member&gt; and
     * ListIterator&lt;Member&gt; implementations.
     */
    protected static abstract class PositionBase extends PositionUnsupported {
        protected PositionBase() {
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
                        extends UnsupportedList<Position> {
        protected PositionListUnsupported() {
        }
    }

    protected static abstract class PositionIterBase
                                extends PositionListUnsupported {
        protected PositionIterBase() {
            super();
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
                                extends PositionListUnsupported {
        protected PositionListBase() {
            super();
        }
        public abstract int size();
        public abstract Position get(int index);

        // Collection
        public boolean isEmpty() {
            return (size() == 0);
        }
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
