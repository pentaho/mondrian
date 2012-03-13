/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2011 Pentaho and others
// Copyright (C) 2007-2007 Tasecurity Group S.L, Spain
// All Rights Reserved.
*/
package mondrian.util;

import java.lang.ref.WeakReference;
import java.util.*;

/**
 * Map with limited size to be used as cache.
 *
 * @author lcanals, www.tasecurity.net
 */
public class CacheMap<S, T> implements Map<S, T> {
    private LinkedNode head;
    private LinkedNode tail;
    private final Map<S, Pair<S, T>> map;
    private final int maxSize;

    /**
     * Creates an empty map with limited size.
     *
     * @param size Maximum number of mapped elements.
     */
    public CacheMap(final int size) {
        this.head = new LinkedNode(null, null);
        this.tail = new LinkedNode(head, null);
        this.map = new WeakHashMap<S, Pair<S, T>>(size);
        this.maxSize = size;
    }

    public void clear() {
        this.head = new LinkedNode(null, null);
        this.tail = new LinkedNode(head, null);
        map.clear();
    }

    public boolean containsKey(final Object key) {
        return map.containsKey(key);
    }

    public boolean containsValue(final Object value) {
        return this.values().contains(value);
    }

    public Set entrySet() {
        final Set<Map.Entry<S, T>> set = new HashSet<Map.Entry<S, T>>();
        for (final Map.Entry<S, Pair<S, T>> entry : this.map.entrySet()) {
            set.add(
                new Map.Entry<S, T>() {
                    public boolean equals(Object s) {
                        if (s instanceof Map.Entry) {
                            return ((Map.Entry) s).getKey().equals(
                                entry.getKey())
                                && ((Map.Entry) s).getValue().equals(
                                    entry.getValue().value);
                        } else {
                            return false;
                        }
                    }

                    public S getKey() {
                        return entry.getKey();
                    }

                    public T getValue() {
                        return entry.getValue().value;
                    }

                    public int hashCode() {
                        return entry.hashCode();
                    }

                    public T setValue(final T x) {
                        return entry.setValue(
                            new Pair<S, T>(
                                x,
                                new LinkedNode(head, entry.getKey()))).value;
                    }
                });
        }
        return set;
    }

    public T get(final Object key) {
        final Pair<S, T> pair = map.get(key);
        if (pair != null) {
            final LinkedNode<S> node = pair.getNode();
            if (node == null) {
                map.remove(key);
                return null;
            }
            node.moveTo(head);
            return pair.value;
        } else {
            return null;
        }
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Set<S> keySet() {
        return map.keySet();
    }

    public T put(final S key, final T value) {
        final Pair<S, T> pair =
            new Pair<S, T>(value, new LinkedNode(head, key));
        final Pair<S, T> obj = map.put(key, pair);
        if (map.size() > maxSize) {
            tail.getPrevious().remove();
            map.remove(key);
        }
        if (obj != null) {
            return obj.value;
        } else {
            return null;
        }
    }

    public void putAll(final Map t) {
        throw new UnsupportedOperationException();
    }

    public T remove(final Object key) {
        final Pair<S, T> pair = map.get(key);
        if (pair == null) {
            return null;
        }
        pair.getNode().remove();
        return map.remove(key).value;
    }

    public int size() {
        return map.size();
    }

    public Collection<T> values() {
        final List<T> vals = new ArrayList<T>();
        for (final Pair<S, T> pair : map.values()) {
            vals.add(pair.value);
        }
        return vals;
    }

    public int hashCode() {
        return map.hashCode();
    }

    public String toString() {
        return "Ordered keys: " + head.toString() + "\n"
                + "Map:" + map.toString();
    }

    public boolean equals(Object o) {
        CacheMap c = (CacheMap) o;
        return map.equals(c.map);
    }

    //
    // PRIVATE STUFF ------------------
    //

    /**
     * Pair of linked key - value
     */
    private final class Pair<S, T> implements java.io.Serializable {
        private final T value;
        private final WeakReference<LinkedNode<S>> node;
        private Pair(final T value, final LinkedNode<S> node) {
            this.node = new WeakReference<LinkedNode<S>>(node);
            this.value = value;
        }

        private LinkedNode<S> getNode() {
            return node.get();
        }

        public boolean equals(final Object o) {
            return o != null && o.equals(this.value);
        }
    }


    /**
     * Represents a node in a linked list.
     */
    private class LinkedNode<S> implements java.io.Serializable {
        private LinkedNode<S> next, prev;
        private S key;

        public LinkedNode(final LinkedNode<S> prev, final S key) {
            this.key = key;
            insertAfter(prev);
        }

        public void remove() {
            if (this.prev != null) {
                this.prev.next = this.next;
            }
            if (this.next != null) {
                this.next.prev = this.prev;
            }
        }

        public void moveTo(final LinkedNode<S> prev) {
            remove();
            insertAfter(prev);
        }

        public LinkedNode<S> getPrevious() {
            return this.prev;
        }

        public String toString() {
            if (this.next != null) {
                if (key != null) {
                    return key.toString() + ", " + this.next.toString();
                } else {
                    return "<null>, " + this.next.toString();
                }
            } else {
                if (key != null) {
                    return key.toString();
                } else {
                    return "<null>";
                }
            }
        }

        private void insertAfter(final LinkedNode<S> prev) {
            if (prev != null) {
                this.next = prev.next;
            } else {
                this.prev = null;
            }
            this.prev = prev;

            if (prev != null) {
                if (prev.next != null) {
                    prev.next.prev = this;
                }
                prev.next = this;
            }
        }
    }
}

// End CacheMap.java
