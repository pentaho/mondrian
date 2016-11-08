/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

//   Copyright (c) 1999-2007 CERN - European Organization for Nuclear Research.
//   Permission to use, copy, modify, distribute and sell this software
//   and its documentation for any purpose is hereby granted without fee,
//   provided that the above copyright notice appear in all copies and
//   that both that copyright notice and this permission notice appear in
//   supporting documentation. CERN makes no representations about the
//   suitability of this software for any purpose. It is provided "as is"
//   without expressed or implied warranty.

// Created from package cern.colt.map by Richard Emberson, 2007/1/23.
// For the source to the Colt project, go to:
// http://dsd.lbl.gov/~hoschek/colt/
package mondrian.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An <code>ObjectPool</code> is a low-memory replacement for a
 * {@link java.util.HashSet}. A HashSet contains a {@link java.util.HashMap}
 * which in turn has
 * an array of Entry objects, the Entry objects themselves, and the
 * key and value objects. An ObjectPool has simply an array of
 * objects and the objects themselves which server as both key and value.
 * <p>
 * This is like the String <code>intern</code> method, but works for
 * an Object type and whereas the String <code>intern</code> method is global
 * an ObjectPool can be used within a context and then garbage collected.
 * Objects can not removed from an ObjectPool except by calling the
 * <code>clear</code> method which removes all objects.
 * <p>
 * Just as with a HashSet's key objects, objects to be placed into
 * an ObjectPool must implement the <code>equals</code> and
 * <code>hashCode</code> methods.
 * <p>
 * This implementation is NOT thread safe.
 *
 * @author Richard Emberson
 */
public class ObjectPool<T> {
    // TODO: Use bits, the state byte array could be a bit array.
    // The Cern code has to use bytes because they also support
    // a REMOVE (== 2) state value but for the ObjectPool we only
    // have FREE or FULL so the use of bits is possible; the
    // state byte array could be a bit vector which would save
    // some memory.
    protected static final byte FREE = 0;
    protected static final byte FULL = 1;

    protected static final int DEFAULT_CAPACITY = 277;
    protected static final double DEFAULT_MIN_LOAD_FACTOR = 0.2;
    protected static final double DEFAULT_MAX_LOAD_FACTOR = 0.5;


    /**
     *  The number of distinct associations in the map; its "size()".
     */
    protected int distinct;

    protected int highWaterMark;

    /**
     * The minimum load factor for the hashtable.
     */
    protected double minLoadFactor;

    /**
     * The maximum load factor for the hashtable.
     */
    protected double maxLoadFactor;

    protected T[] values;

    /**
     * The number of table entries in state==FREE.
     */
    protected int freeEntries;

    public ObjectPool() {
        this(DEFAULT_CAPACITY);
    }

    public ObjectPool(int initialCapacity) {
        this(initialCapacity, DEFAULT_MIN_LOAD_FACTOR, DEFAULT_MAX_LOAD_FACTOR);
    }

    public ObjectPool(
        int initialCapacity,
        double minLoadFactor,
        double maxLoadFactor)
    {
        setUp(initialCapacity, minLoadFactor, maxLoadFactor);
    }

    /**
     * Return the number of entries in the ObjectPool.
     *
     * @return number of entries.
     */
    public int size() {
        return distinct;
    }

    /**
     * Reduce the size of the internal arrays to a size just big
     * enough to hold the current set of entries. Generally, this
     * should only be called after all entries have been added.
     * Calling this causes a new, smaller array to be allocated, the
     * objects are copied to the new array and then the old array is
     * free to be garbage collected; there is a small time when both
     * arrays are in memory.
     */
    public void trimToSize() {
        // * 1.2 because open addressing's performance
        // exponentially degrades beyond that point
        // so that even rehashing the table can take very long
        int newCapacity = nextPrime((int)(1 + 1.2 * size()));
        if (values.length > newCapacity) {
            rehash(newCapacity);
        }
    }

    /**
     * Returns true it the Object is already in the ObjectPool and false
     * otherwise.
     *
     * @param key Object to test if member already or not.
     * @return true is already member
     */
    public boolean contains(T key) {
        int i = indexOfInsertion(key);
        return (i < 0);
    }

    /**
     * Adds an object to the ObjectPool if it is not
     * already in the pool or returns the object that is already in the
     * pool that matches the object being added.
     *
     * @param key Object to add to pool
     * @return Equivalent object, if it exists, otherwise key
     */
    public T add(T key) {
        int i = indexOfInsertion(key);
        if (i < 0) {
            //already contained
            i = -i -1;
            return this.values[i];
        }

        if (this.distinct > this.highWaterMark) {
            int newCapacity = chooseGrowCapacity(
                this.distinct + 1,
                this.minLoadFactor,
                this.maxLoadFactor);
            rehash(newCapacity);
            return add(key);
        }

        T v = this.values[i];
        this.values[i] = key;

        if (v == null) {
            this.freeEntries--;
        }
        this.distinct++;

        if (this.freeEntries < 1) {
            //delta
            int newCapacity = chooseGrowCapacity(
                this.distinct + 1,
                this.minLoadFactor,
                this.maxLoadFactor);
             rehash(newCapacity);
        }

        return key;
    }

    /**
     * Removes all objects from the pool but keeps the current size of
     * the internal storage.
     */
    public void clear() {
        values = (T[]) new Object[values.length];

        this.distinct = 0;
        this.freeEntries = values.length; // delta
        trimToSize();
    }

    /**
     * Returns an Iterator of this <code>ObjectPool</code>. The order of
     * the Objects returned by the <code>Iterator</code> can not be
     * counted on to be in the same order as they were inserted
     * into the <code>ObjectPool</code>.  The
     * <code>Iterator</code> returned does not
     * support the removal of <code>ObjectPool</code> members.
     */
    public Iterator<T> iterator() {
        return new Itr();
    }

    protected int chooseGrowCapacity(int size, double minLoad, double maxLoad) {
        return nextPrime(
            Math.max(
                size + 1,
                (int) ((4 * size / (3 * minLoad + maxLoad)))));
    }

    protected final int chooseHighWaterMark(int capacity, double maxLoad) {
        //makes sure there is always at least one FREE slot
        return Math.min(capacity - 2, (int) (capacity * maxLoad));
    }

    protected final int chooseLowWaterMark(int capacity, double minLoad) {
        return (int) (capacity * minLoad);
    }

/*
    protected int chooseMeanCapacity(int size, double minLoad, double maxLoad) {
        return nextPrime(
            Math.max(
                size + 1,
                (int) ((2 * size/(minLoad + maxLoad)))));
    }

    protected int chooseShrinkCapacity(
        int size, double minLoad, double maxLoad)
    {
        return nextPrime(
            Math.max(
                size + 1,
                (int) ((4 * size / (minLoad + 3 * maxLoad)))));
    }
*/

    protected int nextPrime(int desiredCapacity) {
        return PrimeFinder.nextPrime(desiredCapacity);
    }

    protected void setUp(
        int initialCapacity,
        double minLoadFactor,
        double maxLoadFactor)
    {
        int capacity = initialCapacity;

        if (initialCapacity < 0) {
            throw new IllegalArgumentException(
                "Initial Capacity must not be less than zero: "+
                initialCapacity);
        }
        if (minLoadFactor < 0.0 || minLoadFactor >= 1.0) {
            throw new IllegalArgumentException(
                "Illegal minLoadFactor: " + minLoadFactor);
        }
        if (maxLoadFactor <= 0.0 || maxLoadFactor >= 1.0) {
            throw new IllegalArgumentException(
                "Illegal maxLoadFactor: " + maxLoadFactor);
        }
        if (minLoadFactor >= maxLoadFactor) {
            throw new IllegalArgumentException(
                "Illegal minLoadFactor: " + minLoadFactor
                + " and maxLoadFactor: " + maxLoadFactor);
        }
        capacity = nextPrime(capacity);

        // open addressing needs at least one FREE slot at any time.
        if (capacity == 0) {
            capacity = 1;
        }

        //this.table = new long[capacity];
        this.values = (T[]) new Object[capacity];
        //this.state = new byte[capacity];

        // memory will be exhausted long before this
        // pathological case happens, anyway.
        this.minLoadFactor = minLoadFactor;
        if (capacity == PrimeFinder.largestPrime) {
            this.maxLoadFactor = 1.0;
        } else {
            this.maxLoadFactor = maxLoadFactor;
        }

        this.distinct = 0;
        this.freeEntries = capacity; // delta
        this.highWaterMark = chooseHighWaterMark(capacity, this.maxLoadFactor);
    }


    protected int hash(T key) {
        return (key == null) ? 0 : key.hashCode();
    }
    protected boolean equals(T t, T key) {
        return (t != null) && t.equals(key);
    }

    protected int indexOfInsertion(T key) {
        final T[] tab = values;
        final int length = tab.length;

        final int hash = key.hashCode() & 0x7FFFFFFF;
        int i = hash % length;

        // double hashing,
        // see http://www.eece.unm.edu/faculty/heileman/hash/node4.html
        int decrement = hash % (length - 2);

        //int decrement = (hash / length) % length;
        if (decrement == 0) {
            decrement = 1;
        }

        // stop if we find a free slot, or if we find the key itself
        T v = tab[i];
        while (v != null && !v.equals(key)) {
            //hashCollisions++;
            i -= decrement;
            if (i < 0) {
                i += length;
            }
            v = tab[i];
        }

        // key already contained at slot i.
        // return a negative number identifying the slot.
        // not already contained, should be inserted at slot i.
        // return a number >= 0 identifying the slot.
        return (v != null) ? -i - 1 : i;
    }

    protected void rehash(int newCapacity) {
        int oldCapacity = values.length;

        T[] oldValues = values;

        T[] newValues = (T[]) new Object[newCapacity];

        this.highWaterMark = chooseHighWaterMark(
            newCapacity, this.maxLoadFactor);

        this.values = newValues;
        this.freeEntries = newCapacity - this.distinct; // delta
        for (int i = oldCapacity ; i-- > 0 ;) {
            T v = oldValues[i];
            if (v != null) {
                int index = indexOfInsertion(v);
                newValues[index] = v;
            }
        }
    }

    private class Itr implements Iterator<T> {
        int index = 0;
        Itr() {
        }

        public boolean hasNext() {
            if (index == ObjectPool.this.values.length) {
                return false;
            }
            while (ObjectPool.this.values[index] == null) {
                index++;
                if (index == ObjectPool.this.values.length) {
                    return false;
                }
            }
            return (ObjectPool.this.values[index] != null);
        }

        public T next() {
            if (index >= ObjectPool.this.values.length) {
                throw new NoSuchElementException();
            }
            return ObjectPool.this.values[index++];
        }

        public void remove() {
            throw new UnsupportedOperationException("ObjectPool.Itr.remove");
        }
    }
}

// End ObjectPool.java
