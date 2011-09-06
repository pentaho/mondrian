/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2001-2002 Kana Software, Inc.
// Copyright (C) 2001-2010 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;

import mondrian.olap.*;

import java.util.*;

/**
 * A <code>RolapMember</code> is a member of a {@link RolapHierarchy}. There are
 * sub-classes for {@link RolapStoredMeasure}, {@link RolapCalculatedMember}.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
public interface RolapMember extends Member, RolapCalculation {
    /**
     * Returns the value of this member's key, or throws if the key is
     * composite. Calls to this method should be converted to use one of the
     * other getKeyXxx methods.
     *
     * @return Key value
     */
    Object getKey();

    /**
     * Returns this member's key as a list. Never returns null.
     *
     * <p>For memory efficiency, the keys may be stored in an array (if the
     * key is composite), as a raw object (if the key has a single column), or
     * null (if the key is empty).
     *
     * <p>This method converts the key to a temporary list for operations where
     * it is more uniform to operate on a list than to deal with those special
     * cases.
     *
     * <p>The caller must not modify the list.
     */
    List<Object> getKeyAsList();

    /**
     * Returns this member's key as an array.
     *
     * <p>The result is equivalent to calling getKeyAsList().toArray(); in
     * particular, the result is never null.
     *
     * <p>For memory efficiency, the keys may be stored in an array (if the
     * key is composite), as a raw object (if the key has a single column), or
     * null (if the key is empty).
     * This method converts the key to a temporary list for operations where
     * it is more uniform to operate on a list than to deal with those special
     * cases.
     */
    Object[] getKeyAsArray();

    /**
     * Returns this member in a compact form, and consistent with the
     * specification in {@link RolapMember.Key}.
     *
     * @return Compact representation of this member's key
     */
    Object getKeyCompact();

    RolapMember getParentMember();
    RolapHierarchy getHierarchy();
    RolapLevel getLevel();

    /** @deprecated will be removed in mondrian-4.0 */
    boolean isAllMember();

    /**
     * Collection of static methods to create and manipulate member key values.
     *
     * <p>Member key values must be memory-efficient, quick to create,
     * immutable, and suitable for using in a {@link Map}. (That is, they must
     * implement {@link #equals(Object)} and {@link #hashCode()} correctly.)
     *
     * <p>The format is as follows:
     * <ul>
     * <li>{@link Collections#EMPTY_LIST} if cardinality = 0,</li>
     * <li>an object if cardinality = 1,</li>
     * <li>a list if cardinality &ge; 2</li>
     * </ul>
     *
     * <p>If the key included in members or used as a key in caches, the lists
     * will be flat lists, per {@link Util#flatList(Object[])}. Temporary keys,
     * used to say probe into a map, may use another form of list, such as that
     * returned by {@link Arrays#asList(Object[])}, for efficiency.
     *
     * <p>A key is never null, and neither are any of its components.
     *
     * <p>A key is referred to as <b>simple</b> if cardinality is 1,
     * <b>composite</b> if cardinality &gt; 1 (and sometimes if cardinality =
     * 0).
     */
    static class Key {
        private static final Class<? extends List> LIST2_TYPE =
            Util.flatList("a", "b").getClass();
        private static final Class<? extends List> LIST3_TYPE =
            Util.flatList("a", "b", "c").getClass();
        private static final Class<? extends List> LISTN_TYPE =
            Arrays.asList("").getClass();

        /**
         * Creates a composite or non-composite key value. All components are
         * assumed to be value objects (that is, immutable and implement
         * {@link #equals(Object)} and {@link #hashCode()} correctly).
         *
         * Returns a list of the value is composite, otherwise just the value.
         *
         * @param values Component values
         * @return Value object
         */
        public static Object create(Object[] values) {
            if (values.length == 1) {
                return values[0];
            } else {
                return Util.flatList(values);
            }
        }

        /**
         * Returns whether a key value seems to be consistent with the standard
         * for simple and composite key values. If this method returns false,
         * you should probably change your code to use
         * {@link #create(Object[])}.
         *
         * @param key Key value
         * @param level Level
         * @return Whether key value is valid
         */
        public static boolean isValid(Object key, RolapLevel level) {
            if (level.isAll()) {
                return key.equals(Collections.<Object>emptyList());
            }
            final int keyCount = level.attribute.keyList.size();
            if (key instanceof String
                || key instanceof Number
                || key == RolapUtil.sqlNullValue)
            {
                return keyCount == 1
                    || level.isMeasure() && keyCount == 0;
            }
            if (key instanceof List) {
                List list = (List) key;
                final int arity = list.size();
                if (keyCount != arity) {
                    return false;
                }
                switch (arity) {
                case 0:
                    return key == Collections.EMPTY_LIST;
                case 1:
                    return false;
                case 2:
                    return key.getClass() == LIST2_TYPE;
                case 3:
                    return key.getClass() == LIST3_TYPE;
                default:
                    return key.getClass() == LISTN_TYPE;
                }
            }
            return false;
        }
    }
}

// End RolapMember.java
