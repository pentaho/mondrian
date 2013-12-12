/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;

import java.sql.Timestamp;
import java.util.*;

/**
 * A <code>RolapMember</code> is a member of a {@link RolapHierarchy}. There are
 * sub-classes for {@link RolapStoredMeasure}, {@link RolapCalculatedMember}.
 *
 * <h2>Task list for cleanup</h2>
 *
 * <ul>
 * <li>DONE obsolete {@code RolapCubeMember}. replace all calls with
 * {@link RolapMemberBase} or {@link RolapMember}</li>
 *
 * <li>TODO study and fix
 * {@link mondrian.util.Bug#BugSegregateRolapCubeMemberFixed}</li>
 *
 * <li>DONE remove no-args {@link RolapMemberBase} constructor; it is only
 * called by {@link DelegatingRolapMember}</li>
 *
 * <li>DONE remove {@code mondrian.rolap.RolapMemberInCube}, now equivalent to
 * {@link RolapMember}</li>
 *
 * <li>DONE slim down {@link mondrian.rolap.DelegatingRolapMember} so that it
 * implements {@link RolapMember} but does not extend
 * {@link RolapMemberBase}.</li>
 *
 * <li>DONE remove {@code RolapAllCubeMember}</li>
 *
 * <li>DONE reparent {@link mondrian.rolap.RolapHierarchy.LimitedRollupMember}</li>
 *
 * <li>DONE remove {@link RolapCubeHierarchy}.bootstrapLookup</li>
 *
 * <li>DONE remove {@link RolapCubeHierarchy}.cachingEnabled</li>
 *
 * <li>DONE remove {@code
 * mondrian.rolap.RolapCubeHierarchy.NoCacheRolapCubeHierarchyMemberReader}
 * obsolete</li>
 *
 * <li>DONE remove
 * {@code mondrian.rolap.RolapCubeHierarchy.CacheRolapCubeHierarchyMemberReader}
 * obsolete</li>
 *
 * <li>TODO remove {@link MondrianProperties#EnableRolapCubeMemberCache}</li>
 *
 * <li>DONE move {@link RolapHierarchy}.memberReader down to
 * {@link RolapCubeHierarchy#memberReader}</li>
 *
 * <li>MAYBE move {@link RolapSchemaLoader#createMemberReader} to
 * {@link RolapSchemaLoader}</li>
 *
 * <li>DONE obsolete
 * {@code mondrian.rolap.RolapCubeHierarchy.RolapCubeStoredMeasure}</li>
 *
 * <li>DONE obsolete
 * {@code mondrian.rolap.RolapCubeHierarchy.RolapCubeCalculatedMeasure}</li>
 *
 * <li>DONE obsolete {@code MemberNoCacheHelper}</li>
 *
 * <li>DONE obsolete {@code RolapCubeHierarchy.MemberNoCacheHelper}</li>
 * </ul>
 *
 * <li>Investigate {@link RolapUtil#findBestMemberMatch}</li>
 *
 * @author jhyde
 * @since 10 August, 2001
 */
public interface RolapMember extends Member, RolapCalculation {
    /**
     * Returns the value of this member's key, or throws if the key is
     * composite. Calls to this method should be converted to use one of the
     * other getKeyXxx methods.
     *
     * @return Key value
     */
    Comparable getKey();

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
    List<Comparable> getKeyAsList();

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
     * <p>The result is either a value or a list. Either way, it implements
     * Comparable.
     *
     * @return Compact representation of this member's key
     */
    Comparable getKeyCompact();

    RolapMember getParentMember();
    RolapCubeHierarchy getHierarchy();
    RolapCubeLevel getLevel();

    /** Returns the object that stores annotations and localized strings. */
    Larder getLarder();

    /**
     * Returns the cube this member belongs to.
     *
     * @return Cube this cube member belongs to, never null
     */
    RolapCube getCube();

    RolapCubeDimension getDimension();

    // override with stricter return
    RolapMember getDataMember();

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
            Util.flatList("a", "b", "c", "d").getClass();

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
        public static Comparable create(Comparable[] values) {
            if (values.length == 1) {
                return values[0];
            } else {
                return (Comparable) Util.flatList(values);
            }
        }

        /**
         * Returns whether a key value seems to be consistent with the standard
         * for simple and composite key values. If this method returns false,
         * you should probably change your code to use
         * {@link #create(Comparable[])}.
         *
         * @param key Key value
         * @param level Level
         * @param memberType Member type
         * @return Whether key value is valid
         */
        public static boolean isValid(
            Object key,
            RolapCubeLevel level,
            MemberType memberType)
        {
            if (memberType == MemberType.FORMULA) {
                return key == null;
            }
            if (level.isAll()) {
                return key.equals(Util.COMPARABLE_EMPTY_LIST);
            }
            final int keyCount = level.attribute.getKeyList().size();
            if (key instanceof String
                || key instanceof Number
                || key instanceof Boolean
                || key instanceof Timestamp
                || key instanceof Date
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
                    return key == Util.COMPARABLE_EMPTY_LIST;
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

        /** Returns a representation of a key that is quicker to create than
         * that returned by {@link #create(Comparable[])} but may not implement
         * {@link Comparable}. */
        public static Object quick(Comparable[] keyValues) {
            return keyValues.length == 1
                ? keyValues[0]
                : Arrays.asList(keyValues);
        }
    }
}

// End RolapMember.java
