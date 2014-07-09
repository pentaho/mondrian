/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2010-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import java.util.*;

/**
 * Provides a way to pass objects via a string moniker.
 *
 * <p>This is useful if you are passing an object over an API that admits only
 * strings (such as
 * {@link java.sql.DriverManager#getConnection(String, java.util.Properties)})
 * and where tricks such as {@link ThreadLocal} do not work. The callee needs
 * to be on the same JVM, but other than that, the object does not need to have
 * any special properties. In particular, it does not need to be serializable.
 *
 * <p>First, register the object to obtain a lock box entry. Every lock box
 * entry has a string moniker that is very difficult to guess, is unique, and
 * is not recycled. Pass that moniker to the callee, and from that moniker the
 * callee can retrieve the entry and with it the object.
 *
 * <p>The entry cannot be forged and cannot be copied. If you lose the entry,
 * you can no longer retrieve the object, and the entry will eventually be
 * garbage-collected. If you call {@link #deregister(Entry)}, callees will no
 * longer be able to look up an entry from its moniker.
 *
 * <p>The same is not true of the moniker string. Having the moniker string
 * does not guarantee that the entry will not be removed. Therefore, the
 * creator who called {@link #register(Object)} and holds the entry controls
 * the lifecycle.
 *
 * <p>The moniker consists of the characters A..Z, a..z, 0..9, $, #, and is
 * thus a valid (case-sensitive) identifier.
 *
 * <p>All methods are thread-safe.
 *
 * @author jhyde
  * @since 2010/11/18
 */
public class LockBox {
    private static final Object DUMMY = new Object();

    /**
     * Mapping from monikers to entries.
     *
     * <p>Per WeakHashMap: "An entry in a WeakHashMap will automatically be
     * removed when its key is no longer in ordinary use. More precisely,
     * the presence of a mapping for a given key will not prevent the key
     * from being discarded by the garbage collector, that is, made
     * finalizable, finalized, and then reclaimed. When a key has been
     * discarded its entry is effectively removed from the map..."
     *
     * <p>LockBoxEntryImpl meets those constraints precisely. An entry will
     * disappear when the caller forgets the key, or calls deregister. If
     * the caller (or someone) still has the moniker, it is not sufficient
     * to prevent the entry from being garbage collected.
     */
    private final Map<LockBoxEntryImpl, Object> map =
        new WeakHashMap<LockBoxEntryImpl, Object>();
    private final Random random = new Random();
    private final byte[] bytes = new byte[16]; // 128 bit... secure enough
    private long ordinal;

    /**
     * Creates a LockBox.
     */
    public LockBox() {
    }

    private static Object wrap(Object o) {
        return o == null ? DUMMY : o;
    }

    private static Object unwrap(Object value) {
        return value == DUMMY ? null : value;
    }

    /**
     * Adds an object to the lock box, and returns a key for it.
     *
     * <p>The object may be null. The same object may be registered multiple
     * times; each time it is registered, a new entry with a new string
     * moniker is generated.
     *
     * @param o Object to register. May be null.
     * @return Entry containing the object's string key and the object itself
     */
    public synchronized Entry register(Object o) {
        String moniker = generateMoniker();
        final LockBoxEntryImpl entry = new LockBoxEntryImpl(this, moniker);
        map.put(entry, wrap(o));
        return entry;
    }

    /**
     * Generates a non-repeating, random string.
     *
     * <p>Must be called from synchronized context.
     *
     * @return Non-repeating random string
     */
    private String generateMoniker() {
        // The prefixed ordinal ensures that the string never repeats. Of
        // course, there will be a pattern to the first few chars of the
        // returned string, but that doesn't matter for these purposes.
        random.nextBytes(bytes);

        // Remove trailing '='. It is padding required by base64 spec but does
        // not help us.
        String base64 = Base64.encodeBytes(bytes);
        while (base64.endsWith("=")) {
            base64 = base64.substring(0, base64.length() - 1);
        }
        // Convert '/' to '$' and '+' to '_'. The resulting moniker starts with
        // a '$', and contains only A-Z, a-z, 0-9, _ and $; it is a valid
        // identifier, and does not need to be quoted in XML or an HTTP URL.
        base64 = base64.replace('/', '$');
        base64 = base64.replace('+', '_');
        return "$"
               + Long.toHexString(++ordinal)
               + base64;
    }

    /**
     * Removes an entry from the lock box.
     *
     * <p>It is safe to call this method multiple times.
     *
     * @param entry Entry to deregister
     * @return Whether the object was removed
     */
    public synchronized boolean deregister(Entry entry) {
        return map.remove(entry) != null;
    }

    /**
     * Retrieves an entry using its string moniker. Returns null if there is
     * no entry with that moniker.
     *
     * <p>Successive calls for the same moniker do not necessarily return
     * the same {@code Entry} object, but those entries'
     * {@link LockBox.Entry#getValue()} will nevertheless return the same
     * value.</p>
     *
     * @param moniker Moniker of the lock box entry
     * @return Entry, or null if there is no entry with this moniker
     */
    public synchronized Entry get(String moniker) {
        // Linear scan through keys. Not perfect, but safer than maintaining
        // a map that might mistakenly allow/prevent GC.
        for (LockBoxEntryImpl entry : map.keySet()) {
            if (entry.moniker.equals(moniker)) {
                return entry;
            }
        }
        return null;
    }

    /**
     * Entry in a {@link LockBox}.
     *
     * <p>Entries are created using {@link LockBox#register(Object)}.
     *
     * <p>The object can be retrieved using {@link #getValue()} if you have
     * the entry, or {@link LockBox#get(String)} if you only have the
     * string key.
     *
     * <p>Holding onto an Entry will prevent the entry, and the associated
     * value from being garbage collected. Holding onto the moniker will
     * not prevent garbage collection.</p>
     */
    public interface Entry
    {
        /**
         * Returns the value in this lock box entry.
         *
         * @return Value in this lock box entry.
         */
        Object getValue();

        /**
         * String key by which to identify this object. Not null, not easily
         * forged, and unique within the lock box.
         *
         * <p>Given this moniker, you retrieve the Entry using
         * {@link LockBox#get(String)}. The retrieved Entry will will have the
         * same moniker, and will be able to access the same value.</p>
         *
         * @return String key
         */
        String getMoniker();

        /**
         * Returns whether the entry is still valid. Returns false if
         * {@link LockBox#deregister(mondrian.util.LockBox.Entry)} has been
         * called on this Entry or any entry with the same moniker.
         *
         * @return whether entry is registered
         */
        boolean isRegistered();
    }

    /**
     * Implementation of {@link Entry}.
     *
     * <p>It is important that entries cannot be forged. Therefore this class,
     * and its constructor, are private. And equals and hashCode use object
     * identity.
     */
    private static class LockBoxEntryImpl implements Entry {
        private final LockBox lockBox;
        private final String moniker;

        private LockBoxEntryImpl(LockBox lockBox, String moniker) {
            this.lockBox = lockBox;
            this.moniker = moniker;
        }

        public Object getValue() {
            final Object value = lockBox.map.get(this);
            if (value == null) {
                throw new RuntimeException(
                    "LockBox has no entry with moniker [" + moniker + "]");
            }
            return unwrap(value);
        }

        public String getMoniker() {
            return moniker;
        }

        public boolean isRegistered() {
            return lockBox.map.containsKey(this);
        }
    }
}

// End LockBox.java
