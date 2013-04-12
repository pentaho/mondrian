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

import java.util.*;

/**
 * Implementation of list similar to {@link LinkedList}, but stores elements
 * in chunks of 32 elements.
 *
 * <p>ArrayList has O(n) insertion and deletion into the middle of the list.
 * ChunkList insertion and deletion are O(1).</p>
 */
public class ChunkList<E> extends AbstractSequentialList<E> {
    private static final int HEADER_SIZE = 3;
    private int size;
    private Object[] first;
    private Object[] last;

    private static final int CHUNK_SIZE = 64;
    private static final Integer[] INTEGERS =
        new Integer[CHUNK_SIZE + HEADER_SIZE + 1];

    static {
        for (int i = 0; i < INTEGERS.length; i++) {
            INTEGERS[i] = i;
        }
    }

    /** Creates an empty ChunkList. */
    public ChunkList() {
    }

    /** Creates a ChunkList whose contents are a given Collection. */
    public ChunkList(Collection<E> collection) {
        addAll(collection);
    }

    /** For debugging and testing. */
    boolean isValid(boolean fail) {
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
        return true;
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        return locate(index);
    }

    @Override
    public int size() {
        return size;
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

    private ChunkListIterator locate(int index) {
        if (index == 0) {
            return new ChunkListIterator();
        }
        int n = 0;
        for (Object[] chunk = first;;) {
            final int end = end(chunk);
            final int nextN = n + end - HEADER_SIZE;
            final Object[] next = next(chunk);
            if (nextN > index || next == null) {
                return new ChunkListIterator(
                    chunk, n, index - n - 1 + HEADER_SIZE, end);
            }
            n = nextN;
            chunk = next;
        }
    }

    private class ChunkListIterator implements ListIterator<E> {
        private Object[] chunk;
        private int startIndex;
        private int offset;
        private int end;

        ChunkListIterator() {
            this(null, 0, -1, 0);
        }

        ChunkListIterator(Object[] chunk, int startIndex, int offset, int end) {
            this.chunk = chunk;
            this.startIndex = startIndex;
            this.offset = offset;
            this.end = end;
        }

        public boolean hasNext() {
            return offset + 1 < end
                || (chunk == null
                ? first != null
                : ChunkList.next(chunk) != null);
        }

        public E next() {
            ++offset;
            assert offset <= end;
            if (offset == end) {
                if (chunk == null) {
                    chunk = first;
                } else {
                    chunk = ChunkList.next(chunk);
                    startIndex += (end - HEADER_SIZE);
                }
                if (chunk == null) {
                    throw new NoSuchElementException();
                }
                offset = HEADER_SIZE;
                end = end(chunk);
            }
            return (E) element(chunk, offset);
        }

        public boolean hasPrevious() {
            return offset >= HEADER_SIZE || ChunkList.prev(chunk) != null;
        }

        public E previous() {
            --offset;
            if (offset == HEADER_SIZE - 1) {
                chunk = chunk == null ? last : ChunkList.prev(chunk);
                if (chunk == null) {
                    throw new NoSuchElementException();
                }
                end = end(chunk);
                startIndex -= (end - HEADER_SIZE);
                offset = end - 1;
            }
            return (E) element(chunk, offset);
        }

        public int nextIndex() {
            return startIndex + (offset - HEADER_SIZE) + 1;
        }

        public int previousIndex() {
            return startIndex + (offset - HEADER_SIZE);
        }

        public void remove() {
            --size;
            if (end == HEADER_SIZE + 1) {
                // Chunk is now empty.
                final Object[] prev = prev(chunk);
                final Object[] next = ChunkList.next(chunk);
                if (next == null) {
                    last = prev;
                    if (prev == null) {
                        first = null;
                    } else {
                        setNext(prev, null);
                    }
                    chunk = null;
                    end = HEADER_SIZE;
                    offset = end - 1;
                } else {
                    if (prev == null) {
                        first = next;
                        setPrev(next, null);
                    } else {
                        setNext(prev, next);
                        setPrev(next, prev);
                    }
                    chunk = next;
                    offset = HEADER_SIZE;
                    end = end(next);
                }
                return;
            }
            // Move existing contents down one.
            System.arraycopy(
                chunk, offset + 1, chunk, offset, end - offset - 1);
            --end;
            setElement(chunk, end, null); // allow gc
            setEnd(chunk, end);
            if (offset == end) {
                final Object[] next = ChunkList.next(chunk);
                if (next != null) {
                    startIndex += (end - HEADER_SIZE);
                    chunk = next;
                    offset = HEADER_SIZE - 1;
                    end = end(next);
                }
            }
        }

        public void set(E e) {
            setElement(chunk, offset, e);
        }

        public void add(E e) {
            if (chunk == null || end == CHUNK_SIZE + HEADER_SIZE) {
                // FIXME We create a new chunk, but the next chunk might be
                // less than half full. We should consider using it.
                Object[] newChunk = new Object[CHUNK_SIZE + HEADER_SIZE];
                if (chunk == null) {
                    if (first != null) {
                        setNext(newChunk, first);
                        setPrev(first, newChunk);
                    }
                    first = newChunk;
                    if (last == null) {
                        last = newChunk;
                    }
                } else {
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
                }
                chunk = newChunk;
                end = offset = HEADER_SIZE;
            } else {
                // Move existing contents up one.
                System.arraycopy(
                    chunk, offset, chunk, offset + 1, end - offset);
            }
            setElement(chunk, offset, e);
            ++end;
            setEnd(chunk, end);
            ++size;
        }
    }
}

// End ChunkList.java
