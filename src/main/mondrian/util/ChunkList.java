/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import java.lang.reflect.Array;
import java.util.*;

/**
 * Implementation of list similar to {@link LinkedList}, but stores elements
 * in chunks of 64 elements. (This data structure is sometimes known as an
 * "unrolled (double) linked list".
 *
 * <p>ArrayList has O(n) insertion and deletion into the middle of the list.
 * ChunkList insertion and deletion are O(1).</p>
 *
 * <p>Chunks do not have to be full. When a list is created by appending to the
 * end, chunks are created full. However an insertion or deletion will cause
 * gaps. An insertion into a full chunk will cause the chunk to split. A
 * deletion may cause a chunk to merge with the following chunk.</p>
 */
public class ChunkList<E> extends AbstractSequentialList<E>
    // implements Deque<E>
{
    /** Number of elements in the chunk header. The elements are the previous
     * link, next link, and the size of the chunk. */
    private static final int HEADER_SIZE = 3;

    /** Number of elements in a chunk. This does not include the header. Any
     * value would work, but the value should probably be tuned to match the
     * L1 or L2 cache line size, by running some performance tests. */
    private static final int CHUNK_SIZE = 64;

    /** Size at which we consider merging a chunk with its successor. The merge
     * will go ahead if the combined size is less than {@link #POST_MERGE_SIZE}.
     */
    private static final int LOW_SIZE = CHUNK_SIZE / 3;

    /** Minimum size of a chunk after a merge. Must be between
     * {@link #LOW_SIZE} * 2 and {@link #CHUNK_SIZE}. */
    private static final int POST_MERGE_SIZE = CHUNK_SIZE * 2 / 3;

     /** Cached integers, for all possible chunk sizes. For all elements,
     * {@code INTEGERS[i].intValue() == i} holds. */
    private static final Integer[] INTEGERS =
        new Integer[CHUNK_SIZE + HEADER_SIZE + 1];

    /** Number of elements in this list. */
    private int size;

    /** First chunk in this list. Null if and only if the list is empty. */
    private Object[] first;

    /** Last chunk in this list. Null if and only if the list is empty. */
    private Object[] last;

    static {
        for (int i = 0; i < INTEGERS.length; i++) {
            INTEGERS[i] = i;
        }
        assert LOW_SIZE > 0;
        assert LOW_SIZE < CHUNK_SIZE;
        assert POST_MERGE_SIZE > LOW_SIZE;
    }

    /** Creates an empty ChunkList. */
    public ChunkList() {
    }

    /** Creates a ChunkList whose contents are a given Collection. */
    public ChunkList(Collection<E> collection) {
        addAll(collection);
    }

    /** For debugging and testing. */
    boolean isValid(boolean print, boolean fail) {
        if ((first == null) != (last == null)) {
            assert !fail;
            return false;
        }
        if ((first == null) != (size == 0)) {
            assert !fail;
            return false;
        }
        int n = 0;
        for (E e : this) {
            if (n++ > size) {
                assert !fail;
                return false;
            }
        }
        if (n != size) {
            assert !fail;
            return false;
        }
        Object[] prev = null;
        for (Object[] chunk = first; chunk != null; chunk = next(chunk)) {
            if (prev(chunk) != prev) {
                assert !fail;
                return false;
            }
            prev = chunk;
            if (end(chunk) == HEADER_SIZE) {
                assert !fail : "chunk is empty";
                return false;
            }
        }
        if (print) {
            System.out.println(chunkSizeDistribution());
        }
        return true;
    }

    /** For debugging and testing. */
    String chunkSizeDistribution() {
        final ArrayList<Integer> list = new ArrayList<Integer>();
        int n = 0;
        for (Object[] chunk = first; chunk != null; chunk = next(chunk)) {
            ++n;
            final int size1 = end(chunk) - HEADER_SIZE;
            while (size1 > list.size() - 1) {
                list.add(null);
            }
            final Integer integer = list.get(size1);
            if (integer == null) {
                list.set(size1, 1);
            } else {
                list.set(size1, integer + 1);
            }
        }
        final List<String> strings = new ArrayList<String>();
        for (int i = 0; i < list.size(); i++) {
            Integer integer = list.get(i);
            if (integer != null) {
                strings.add(i + ":" + integer);
            }
        }
        return "size: " + size()
            + ", distribution: " + strings
            + ", chunks: " + n
            + ", elements per chunk: " + ((float) size / (float) n);
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }
        return locate(index);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public void clear() {
        // Similar rationale to LinkedList.clear. Clearing links is not strictly
        // necessary, but helps gc.
        for (Object[] x = first; x != null;) {
            Object[] next = next(x);
            setNext(x, null);
            setPrev(x, null);
            x = next;
        }
        first = last = null;
        size = 0;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        if (index != size() || c.isEmpty()) {
            return super.addAll(index, c);
        }
        // Efficient addAll to end
        Object[] chunk;
        int end;
        if (last == null) {
            chunk = new Object[HEADER_SIZE + CHUNK_SIZE];
            end = HEADER_SIZE;
        } else {
            chunk = last;
            end = end(chunk);
        }
        for (E e : c) {
            if (end == HEADER_SIZE + CHUNK_SIZE) {
                Object[] prev = chunk;
                chunk = new Object[HEADER_SIZE + CHUNK_SIZE];
                end = HEADER_SIZE;
                setNext(prev, chunk);
                setPrev(chunk, prev);
            }
            chunk[end++] = e;
        }
        last = chunk;
        setEnd(chunk, end);
        return true;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        final int size = size();
        @SuppressWarnings("unchecked")
        T[] r = a.length >= size
            ? a
            : (T[]) Array.newInstance(a.getClass().getComponentType(), size);
        populateArray(r);
        if (r.length > size) {
            r[size] = null;
        }
        return r;
    }

    @Override
    public Object[] toArray() {
        final Object[] a = new Object[size];
        populateArray(a);
        return a;
    }

    private <T> void populateArray(T[] r) {
        if (first == null) {
            return;
        }
        int start = 0;
        for (Object[] chunk = first;;) {
            final int end = end(chunk);
            final int count = end - HEADER_SIZE;
            System.arraycopy(chunk, HEADER_SIZE, r, start, count);
            chunk = next(chunk);
            if (chunk == null) {
                break;
            }
            start += count;
        }
    }

    @Override
    public boolean contains(Object o) {
        return locate((E) o, true) != null;
    }

    @Override
    public int indexOf(Object o) {
        final ChunkListIterator listIterator = locate((E) o, true);
        if (listIterator == null) {
            return -1;
        }
        return listIterator.nextIndex();
    }

    @Override
    public int lastIndexOf(Object o) {
        final ChunkListIterator listIterator = locate((E) o, false);
        if (listIterator == null) {
            return -1;
        }
        return listIterator.nextIndex();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        if (c.isEmpty()) {
            return true;
        }
        final Set set = new HashSet(c);
        for (E e : this) {
            if (set.remove(e)) {
                if (set.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean add(E element) {
        Object[] chunk = last;
        int end;
        if (chunk == null) {
            chunk = first = last = new Object[CHUNK_SIZE + HEADER_SIZE];
            end = HEADER_SIZE;
        } else {
            end = end(chunk);
            if (end == CHUNK_SIZE + HEADER_SIZE) {
                chunk = new Object[CHUNK_SIZE + HEADER_SIZE];
                setNext(last, chunk);
                setPrev(chunk, last);
                end = HEADER_SIZE;
                last = chunk;
            }
        }
        setEnd(chunk, end + 1);
        setElement(chunk, end, element);
        ++size;
        return true;
    }

    @Override
    public void add(int index, E element) {
        if (index == size) {
            add(element);
        } else {
            super.add(index, element);
        }
    }

    public void addFirst(E e) {
        add(0, e);
    }

    public void addLast(E e) {
        add(size(), e);
    }

    public boolean offerFirst(E e) {
        addFirst(e);
        return true;
    }

    public boolean offerLast(E e) {
        addLast(e);
        return true;
    }

    public E removeFirst() {
        return xxxFirst(true);
    }

    public E pollFirst() {
        return xxxFirst(false);
    }

    private E xxxFirst(boolean throwOnEmpty) {
        if (isEmpty()) {
            if (throwOnEmpty) {
                throw new NoSuchElementException();
            } else {
                return null;
            }
        }
        final ListIterator<E> iterator = listIterator(0);
        E e = iterator.next();
        iterator.remove();
        return e;
    }

    public E removeLast() {
        return xxxLast(true);
    }

    public E pollLast() {
        return xxxLast(false);
    }

    private E xxxLast(boolean throwOnEmpty) {
        if (isEmpty()) {
            if (throwOnEmpty) {
                throw new NoSuchElementException();
            } else {
                return null;
            }
        }
        final ListIterator<E> iterator = listIterator(size);
        E e = iterator.previous();
        iterator.remove();
        return e;
    }

    public E getFirst() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return get(0);
    }

    public E getLast() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }
        return get(size - 1);
    }

    public E peekFirst() {
        if (isEmpty()) {
            return null;
        }
        return get(0);
    }

    public E peekLast() {
        if (isEmpty()) {
            return null;
        }
        return get(size - 1);
    }

    public boolean removeFirstOccurrence(Object o) {
        for (ListIterator<E> i = listIterator(0); i.hasNext();) {
            E e = i.next();
            if (e.equals(o)) {
                i.remove();
                return true;
            }
        }
        return false;
    }

    public boolean removeLastOccurrence(Object o) {
        for (ListIterator<E> i = listIterator(size - 1); i.hasPrevious();) {
            E e = i.previous();
            if (e.equals(o)) {
                i.remove();
                return true;
            }
        }
        return false;
    }

    public boolean offer(E e) {
        return add(e);
    }

    public E remove() {
        return removeFirst();
    }

    public E poll() {
        return pollFirst();
    }

    public E element() {
        return getFirst();
    }

    public E peek() {
        return peekFirst();
    }

    public void push(E e) {
        addFirst(e);
    }

    public E pop() {
        return removeFirst();
    }

    public Iterator<E> descendingIterator() {
        return new DescendingIterator();
    }

    // Static helper methods

    private static Object[] prev(Object[] chunk) {
        return (Object[]) chunk[0];
    }

    private static void setPrev(Object[] chunk, Object[] prev) {
        chunk[0] = prev;
    }

    private static Object[] next(Object[] chunk) {
        return (Object[]) chunk[1];
    }

    private static void setNext(Object[] chunk, Object[] next) {
        assert chunk != next;
        chunk[1] = next;
    }

    private static int end(Object[] chunk) {
        return (Integer) chunk[2];
    }

    private static void setEnd(Object[] chunk, int size) {
        chunk[2] = INTEGERS[size];
    }

    private static Object element(Object[] chunk, int index) {
        return chunk[index];
    }

    private static void setElement(Object[] chunk, int index, Object element) {
        chunk[index] = element;
    }

    /** Returns a ChunkListIterator positioned at a given index, never null.
     * Index must be between 0 and size (inclusive). */
    private ChunkListIterator locate(int index) {
        if (first == null) {
            return new ChunkListIterator();
        }
        if (index * 2 <= size) {
            // index < size / 2, search forwards
            int n = 0;
            for (Object[] chunk = first;;) {
                final int end = end(chunk);
                final int nextN = n + end - HEADER_SIZE;
                final Object[] next = next(chunk);
                if (nextN > index || next == null) {
                    return new ChunkListIterator(
                        chunk, n, index - n + HEADER_SIZE, end);
                }
                n = nextN;
                chunk = next;
            }
        } else {
            // index > size / 2, search backwards
            int n = size;
            for (Object[] chunk = last;;) {
                final int end = end(chunk);
                final int start = n - (end - HEADER_SIZE);
                final Object[] prev = prev(chunk);
                if (start <= index) {
                    return new ChunkListIterator(
                        chunk, n, index - start + HEADER_SIZE, end);
                }
                n = start;
                chunk = prev;
            }
        }
    }

    /** Returns a ChunkListIterator positioned on the first (or last, if forward
     * is false) element that equals a given element. Null if not found. */
    private ChunkListIterator locate(E element, boolean forward) {
        if (first == null) {
            return null;
        }
        if (forward) {
            int start = 0;
            for (Object[] chunk = first;;) {
                final int end = end(chunk);
                if (element == null) {
                    for (int i = HEADER_SIZE; i < end; i++) {
                        if (chunk[i] == null) {
                            return new ChunkListIterator(
                                chunk, start, i, end);
                        }
                    }
                } else {
                    for (int i = HEADER_SIZE; i < end; i++) {
                        if (element.equals(chunk[i])) {
                            return new ChunkListIterator(
                                chunk, start, i, end);
                        }
                    }
                }
                chunk = next(chunk);
                if (chunk == null) {
                    return null;
                }
                start += (end - HEADER_SIZE);
            }
        } else {
            int top = size;
            for (Object[] chunk = last;;) {
                final int end = end(chunk);
                final int start = top - (end - HEADER_SIZE);
                if (element == null) {
                    for (int i = end - 1; i >= HEADER_SIZE; i--) {
                        if (chunk[i] == null) {
                            return new ChunkListIterator(
                                chunk, start, i, end);
                        }
                    }
                } else {
                    for (int i = end - 1; i >= HEADER_SIZE; i--) {
                        if (element.equals(chunk[i])) {
                            return new ChunkListIterator(
                                chunk, start, i, end);
                        }
                    }
                }
                chunk = prev(chunk);
                if (chunk == null) {
                    return null;
                }
                top = start;
            }
        }
    }

    private class ChunkListIterator implements ListIterator<E> {
        private Object[] chunk; // current chunk; null iff list is empty
        private int startIndex; // index in whole list of first element of chunk
        private int nextIndex; // offset within chunk
        private int lastReturned;
        private Object[] lastReturnedChunk;
        private int end; // above the highest occupied element in chunk

        ChunkListIterator() {
            this(null, 0, 0, 0);
        }

        ChunkListIterator(
            Object[] chunk, int startIndex, int nextIndex, int end)
        {
            this.chunk = chunk;
            this.startIndex = startIndex;
            this.nextIndex = nextIndex;
            this.end = end;
            this.lastReturnedChunk = null;
        }

        public boolean hasNext() {
            return nextIndex < end;
        }

        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            lastReturned = nextIndex;
            lastReturnedChunk = chunk;
            final E element = (E) element(chunk, nextIndex);
            ++nextIndex;
            if (nextIndex == end) {
                chunk = ChunkList.next(chunk);
                startIndex += (end - HEADER_SIZE);
                if (chunk == null) {
                    end = HEADER_SIZE;
                } else {
                    end = end(chunk);
                }
                nextIndex = HEADER_SIZE;
            }
            return element;
        }

        public boolean hasPrevious() {
            return nextIndex >= HEADER_SIZE || ChunkList.prev(chunk) != null;
        }

        public E previous() {
            final E element = (E) element(chunk, nextIndex);
            lastReturned = nextIndex;
            lastReturnedChunk = chunk;
            --nextIndex;
            if (nextIndex == HEADER_SIZE - 1) {
                chunk = chunk == null ? last : ChunkList.prev(chunk);
                if (chunk == null) {
                    throw new NoSuchElementException();
                }
                end = end(chunk);
                startIndex -= (end - HEADER_SIZE);
                nextIndex = end - 1;
            }
            return element;
        }

        public int nextIndex() {
            return startIndex + (nextIndex - HEADER_SIZE);
        }

        public int previousIndex() {
            return startIndex + (nextIndex - HEADER_SIZE) - 1;
        }

        public void remove() {
            if (lastReturnedChunk == null) {
                throw new IllegalStateException();
            }
            --size;
            if (lastReturnedChunk == chunk) {
                remove_(end, chunk, lastReturned, true);
            } else {
                // Working in a different block.
                int lastReturnedEnd = end(lastReturnedChunk);
                remove_(
                    lastReturnedEnd, lastReturnedChunk, lastReturned, false);
            }
            lastReturnedChunk = null;
        }

        private void remove_(int end, Object[] chunk, int pos, boolean b) {
            if (end == HEADER_SIZE + 1) {
                // Chunk is now empty. Remove it from the list.
                final Object[] prev = prev(chunk);
                final Object[] next = ChunkList.next(chunk);
                if (next == null) {
                    last = prev;
                    if (prev == null) {
                        first = null;
                    } else {
                        setNext(prev, null);
                    }
                    if (b) {
                        this.chunk = null;
                        this.end = this.nextIndex = HEADER_SIZE;
                    }
                } else {
                    if (prev == null) {
                        first = next;
                        setPrev(next, null);
                    } else {
                        setNext(prev, next);
                        setPrev(next, prev);
                    }
                    if (b) {
                        this.chunk = next;
                        this.nextIndex = HEADER_SIZE;
                        this.end = end(next);
                    }
                }
            } else {
                // Move existing contents down one.
                System.arraycopy(chunk, pos + 1, chunk, pos, end - pos - 1);
                --end;
                setElement(chunk, end, null); // allow gc
                setEnd(chunk, end);
                if (b) {
                    if (nextIndex == end) {
                        final Object[] next = ChunkList.next(chunk);
                        if (next != null) {
                            startIndex += (end - HEADER_SIZE);
                            this.chunk = next;
                            this.nextIndex = HEADER_SIZE;
                            this.end = end(next);
                        }
                    } else {
                        this.end = end;
                    }
                }
            }
        }

        public void set(E e) {
            if (lastReturnedChunk == null) {
                throw new IllegalStateException();
            }
            setElement(lastReturnedChunk, lastReturned, e);
            lastReturnedChunk = null;
        }

        public void add(E e) {
            if (nextIndex == end) {
                if (chunk == null) {
                    // Allocate a new chunk.
                    Object[] newChunk = new Object[CHUNK_SIZE + HEADER_SIZE];
                    assert first == null && last == null;
                    last = first = chunk = newChunk;
                    end = nextIndex = HEADER_SIZE;
                } else if (end < CHUNK_SIZE + HEADER_SIZE) {
                    // There's room to grow. Add to end of chunk.
                } else {
                    // Allocate a new chunk.
                    Object[] newChunk = new Object[CHUNK_SIZE + HEADER_SIZE];
                    final Object[] next = ChunkList.next(chunk);
                    setPrev(newChunk, chunk);
                    setNext(chunk, newChunk);

                    if (next == null) {
                        last = newChunk;
                    } else {
                        setPrev(next, newChunk);
                        setNext(newChunk, next);
                    }
                    startIndex += CHUNK_SIZE;
                    chunk = newChunk;
                    end = nextIndex = HEADER_SIZE;
                }
            } else {
                if (end == CHUNK_SIZE + HEADER_SIZE) {
                    // Split chunk. Pieces are of equal size if CHUNK_SIZE is
                    // even.
                    Object[] newChunk = new Object[CHUNK_SIZE + HEADER_SIZE];
                    final Object[] next = ChunkList.next(chunk);
                    setNext(newChunk, next);
                    if (next != null) {
                        setPrev(next, newChunk);
                    }
                    setPrev(newChunk, chunk);
                    setNext(chunk, newChunk);
                    final int newSize = CHUNK_SIZE / 2;
                    final int remainingEnd = end - newSize;
                    System.arraycopy(
                        chunk, remainingEnd, newChunk, HEADER_SIZE, newSize);
                    Arrays.fill(chunk, remainingEnd, end, null);
                    if (nextIndex <= remainingEnd) {
                        // Point stays within this chunk.
                        end = remainingEnd;
                        setEnd(newChunk, HEADER_SIZE + newSize);
                    } else {
                        // Point is within the new chunk.
                        setEnd(chunk, remainingEnd);
                        chunk = newChunk;
                        end = HEADER_SIZE + newSize;
                        nextIndex -= (remainingEnd - HEADER_SIZE);
                    }
                }
                // Move existing contents up one.
                System.arraycopy(
                    chunk, nextIndex, chunk, nextIndex + 1, end - nextIndex);
            }
            setElement(chunk, nextIndex, e);
            ++end;
            setEnd(chunk, end);
            ++size;
        }
    }

    private class DescendingIterator implements Iterator<E> {
        private final ChunkListIterator iterator = locate(size());

        public boolean hasNext() {
            return iterator.hasPrevious();
        }

        public E next() {
            return iterator.previous();
        }

        public void remove() {
            iterator.remove();
        }
    }
}

// End ChunkList.java
