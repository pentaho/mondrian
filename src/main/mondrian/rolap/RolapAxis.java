/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/

package mondrian.rolap;


import mondrian.olap.Axis;
import mondrian.olap.Member;
import mondrian.olap.Position;
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
        private final Iterable<Member> iter;
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
                list = list;
            }
        }

        
        /** 
         * This List<Position> starts life with a List<Position> implementation
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

        // List<Position>
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
                        Member m = member;
                        member = null;
                        return m;
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
        private final Iterable<Member[]> iter;
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
                list = list;
                len = list.get(0).length;
            }
        }

        // List<Position>
        class PositionWrapper extends PositionListUnsupported {
            List<Position> positionList;
            PositionWrapper() {
                positionList = new PositionIter();
            }
            protected synchronized void materialize() {
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

        // List<Position>
        class PositionIter extends PositionIterBase {
            private Iterator<Member[]> it;
            PositionIter() {
                it = iter.iterator();
            }
            public Iterator<Position> iterator() {
                return new Iterator<Position>() {
                    int hasNextCnt = 0;
                    int nextCnt = 0;
                    public boolean hasNext() {
                        hasNextCnt++;
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
         *  Each Position has a single Member.
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
            this.len = list.get(0).length;
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

    protected static abstract class PositionUnsupported implements Position {
        protected PositionUnsupported() {
        }
        public int size() {
            throw new UnsupportedOperationException("size");
        }
        public Member get(int index) {
            throw new UnsupportedOperationException("get");
        }
        public Member set(int index, Member element) {
            throw new UnsupportedOperationException("set");
        }

        // Collection 
        public boolean isEmpty() {
            return (size() == 0);
        }
        public Object[] toArray() {
            throw new UnsupportedOperationException("toArray");
        }

        public void add(int index, Member element) {
            throw new UnsupportedOperationException("add");
        }
        public Member remove(int index) {
            throw new UnsupportedOperationException("remove");
        }
        public int indexOf(Object o) {
            throw new UnsupportedOperationException("indexOf"); 
        }
        public int lastIndexOf(Object o) {
            throw new UnsupportedOperationException("lastIndexOf");
        }
        public List<Member> subList(int fromIndex, int toIndex) {
            throw new UnsupportedOperationException("subList");
        }

        public boolean contains(Object o) {
            throw new UnsupportedOperationException("contains");
        }
        public <Member> Member[] toArray(Member[] a) {
            throw new UnsupportedOperationException("toArray");
        }
        public boolean add(Member o) {
            throw new UnsupportedOperationException("add");
        }
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("remove");
        }
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException("containsAll");
        }
        public boolean addAll(Collection<? extends Member> c) {
            throw new UnsupportedOperationException("addAll");
        }
        public boolean addAll(int index, Collection<? extends Member> c) {
            throw new UnsupportedOperationException("addAll");
        }
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("removeAll");
        }
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("retainAll");
        }
        public void clear() {
            throw new UnsupportedOperationException("retainAll");
        }
        public boolean equals(Object o) {
            throw new UnsupportedOperationException("equals");
        }
        public int hashCode() {
            throw new UnsupportedOperationException("hashCode");
        }
        public ListIterator<Member> listIterator() {
            throw new UnsupportedOperationException("listIterator");
        }
        public ListIterator<Member> listIterator(int index) {
            throw new UnsupportedOperationException("listIterator");
        }
        public Iterator<Member> iterator() {
            throw new UnsupportedOperationException("iterator");
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
        private class Itr implements Iterator<Member> {
            int cursor = 0;
            int lastRet = -1;

            public boolean hasNext() {
                //return (cursor < size());
                return (cursor != size());
            }
            public Member next() {
                try { 
                    Member next = get(cursor);
                    lastRet = cursor++;
                    return next;
                } catch(IndexOutOfBoundsException e) {
System.out.println("RolapAxis.PositionBase.Itr: cursor=" +cursor);
System.out.println("RolapAxis.PositionBase.Itr: size=" +size());
                    throw new NoSuchElementException();
                }
            }
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        }
        private class ListItr extends Itr implements ListIterator<Member> {
            ListItr(int index) {
                cursor = index;
            }

            public boolean hasPrevious() {
                return cursor != 0;
            }
            public Member previous() {
                try {
                    int i = cursor - 1;
                    Member previous = get(i);
                    lastRet = cursor = i;
                    return previous;
                } catch(IndexOutOfBoundsException e) {
                    throw new NoSuchElementException();
                }
            }

            public int nextIndex() {
                return cursor;
            }

            public int previousIndex() {
                return cursor-1;
            }

            public void set(Member o) {
/*
                if (lastRet == -1)
                    throw new IllegalStateException();
                try {
                    MemberList.this.set(lastRet, o);
                } catch(IndexOutOfBoundsException e) {
                    throw new ConcurrentModificationException();
                }
*/
                throw new UnsupportedOperationException("set");
            }

            public void add(Member o) {
                throw new UnsupportedOperationException("add");
            }
        }
    }


    protected static abstract class PositionListUnsupported 
                implements List<Position> {
        protected PositionListUnsupported() {
        }
        public int size() {
            throw new UnsupportedOperationException("size");
        }
        public Position get(int index) {
            throw new UnsupportedOperationException("get");
        }
        public Position set(int index, Position element) {
            throw new UnsupportedOperationException("set");
        }

        // Collection 
        public boolean isEmpty() {
            return (size() == 0);
        }
        public Object[] toArray() {
            throw new UnsupportedOperationException("toArray");
        }

        public void add(int index, Position element) {
            throw new UnsupportedOperationException("add");
        }
        public Position remove(int index) {
            throw new UnsupportedOperationException("remove");
        }
        public int indexOf(Object o) {
            throw new UnsupportedOperationException("indexOf"); 
        }
        public int lastIndexOf(Object o) {
            throw new UnsupportedOperationException("lastIndexOf");
        }
        public List<Position> subList(int fromIndex, int toIndex) {
            throw new UnsupportedOperationException("subList");
        }

        public boolean contains(Object o) {
            throw new UnsupportedOperationException("contains");
        }
        public <Position> Position[] toArray(Position[] a) {
            throw new UnsupportedOperationException("toArray");
        }
        public boolean add(Position o) {
            throw new UnsupportedOperationException("add");
        }
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("remove");
        }
        public boolean containsAll(Collection<?> c) {
            throw new UnsupportedOperationException("containsAll");
        }
        public boolean addAll(Collection<? extends Position> c) {
            throw new UnsupportedOperationException("addAll");
        }
        public boolean addAll(int index, Collection<? extends Position> c) {
            throw new UnsupportedOperationException("addAll");
        }
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("removeAll");
        }
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("retainAll");
        }
        public void clear() {
            throw new UnsupportedOperationException("retainAll");
        }
        public boolean equals(Object o) {
            throw new UnsupportedOperationException("equals");
        }
        public int hashCode() {
            throw new UnsupportedOperationException("hashCode");
        }
        public ListIterator<Position> listIterator() {
            throw new UnsupportedOperationException("listIterator");
        }
        public ListIterator<Position> listIterator(int index) {
            throw new UnsupportedOperationException("listIterator");
        }
        public Iterator<Position> iterator() {
            throw new UnsupportedOperationException("iterator");
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
        private class Itr implements Iterator<Position> {
            int cursor = 0;
            int lastRet = -1;

            public boolean hasNext() {
                //return (cursor < size());
                return (cursor != size());
            }
            public Position next() {
                try { 
                    Position next = get(cursor);
                    lastRet = cursor++;
                    return next;
                } catch(IndexOutOfBoundsException e) {
System.out.println("RolapAxis.PositionListBase.Itr: cursor=" +cursor);
System.out.println("RolapAxis.PositionListBase.Itr: size=" +size());
                    throw new NoSuchElementException();
                }
            }
            public void remove() {
                throw new UnsupportedOperationException("remove");
            }
        }
        private class ListItr extends Itr implements ListIterator<Position> {
            ListItr(int index) {
                cursor = index;
            }

            public boolean hasPrevious() {
                return cursor != 0;
            }
            public Position previous() {
                try {
                    int i = cursor - 1;
                    Position previous = get(i);
                    lastRet = cursor = i;
                    return previous;
                } catch(IndexOutOfBoundsException e) {
                    throw new NoSuchElementException();
                }
            }

            public int nextIndex() {
                return cursor;
            }

            public int previousIndex() {
                return cursor-1;
            }

            public void set(Position o) {
/*
                if (lastRet == -1)
                    throw new IllegalStateException();
                try {
                    MemberList.this.set(lastRet, o);
                } catch(IndexOutOfBoundsException e) {
                    throw new ConcurrentModificationException();
                }
*/
                throw new UnsupportedOperationException("set");
            }

            public void add(Position o) {
                throw new UnsupportedOperationException("add");
            }
        }
    }

    protected RolapAxis() {
    }
    public abstract List<Position> getPositions();
}
// End RolapAxis.java
