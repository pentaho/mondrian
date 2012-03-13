/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1998-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import java.util.*;

/**
 * <code>EnumeratedValues</code> is a helper class for declaring a set of
 * symbolic constants which have names, ordinals, and possibly descriptions.
 * The ordinals do not have to be contiguous.
 *
 * <p>Typically, for a particular set of constants, you derive a class from this
 * interface, and declare the constants as <code>public static final</code>
 * members. Give it a private constructor, and a <code>public static final
 * <i>ClassName</i> instance</code> member to hold the singleton instance.
 * {@link Access} is a simple example of this.</p>
 */
public class EnumeratedValues<V extends EnumeratedValues.Value>
    implements Cloneable
{
    /** Map symbol names to values */
    private Map<String, V> valuesByName = new LinkedHashMap<String, V>();

    /** the smallest ordinal value */
    private int min = Integer.MAX_VALUE;

    /** the largest ordinal value */
    private int max = Integer.MIN_VALUE;

    // the variables below are only set AFTER makeImmutable() has been called

    /** An array mapping ordinals to {@link Value}s. It is biased by the
     * min value. It is built by {@link #makeImmutable}. */
    private Value[] ordinalToValueMap;
    private static final String[] emptyStringArray = new String[0];

    /**
     * Creates a new empty, mutable enumeration.
     */
    public EnumeratedValues() {
    }

    /**
     * Creates an enumeration, with an array of values, and freezes it.
     */
    public EnumeratedValues(V[] values) {
        for (V value : values) {
            register(value);
        }
        makeImmutable();
    }

    /**
     * Creates an enumeration, initialize it with an array of strings, and
     * freezes it.
     */
    public EnumeratedValues(String[] names) {
        for (int i = 0; i < names.length; i++) {
            register((V) new BasicValue(names[i], i, names[i]));
        }
        makeImmutable();
    }

    /**
     * Create an enumeration, initializes it with arrays of code/name pairs,
     * and freezes it.
     */
    public EnumeratedValues(String[] names, int[] codes) {
        for (int i = 0; i < names.length; i++) {
            register((V) new BasicValue(names[i], codes[i], names[i]));
        }
        makeImmutable();
    }

    /**
     * Create an enumeration, initializes it with arrays of code/name pairs,
     * and freezes it.
     */
    public EnumeratedValues(String[] names, int[] codes, String[] descriptions)
    {
        for (int i = 0; i < names.length; i++) {
            register((V) new BasicValue(names[i], codes[i], descriptions[i]));
        }
        makeImmutable();
    }

    public EnumeratedValues(Class<? extends Enum> clazz) {
        throw new UnsupportedOperationException();
    }

    public EnumeratedValues<V> clone() {
        EnumeratedValues clone;
        try {
            clone = (EnumeratedValues) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw Util.newInternal(ex, "error while cloning " + this);
        }
        clone.valuesByName = new HashMap<String, Value>(valuesByName);
        clone.ordinalToValueMap = null;
        return clone;
    }

    /**
     * Creates a mutable enumeration from an existing enumeration, which may
     * already be immutable.
     */
    public EnumeratedValues getMutableClone() {
        return clone();
    }

    /**
     * Associates a symbolic name with an ordinal value.
     *
     * @pre value != null
     * @pre !isImmutable()
     * @pre value.getName() != null
     */
    public void register(V value) {
        assert value != null : "pre: value != null";
        Util.assertPrecondition(!isImmutable(), "isImmutable()");
        final String name = value.getName();
        Util.assertPrecondition(name != null, "value.getName() != null");
        Value old = valuesByName.put(name, value);
        if (old != null) {
            throw Util.newInternal(
                "Enumeration already contained a value '" + old.getName()
                + "'");
        }
        final int ordinal = value.getOrdinal();
        min = Math.min(min, ordinal);
        max = Math.max(max, ordinal);
    }

    /**
     * Freezes the enumeration, preventing it from being further modified.
     */
    public void makeImmutable() {
        ordinalToValueMap = new Value[1 + max - min];
        for (Value value : valuesByName.values()) {
            final int index = value.getOrdinal() - min;
            if (ordinalToValueMap[index] != null) {
                throw Util.newInternal(
                    "Enumeration has more than one value with ordinal "
                    + value.getOrdinal());
            }
            ordinalToValueMap[index] = value;
        }
    }

    public final boolean isImmutable() {
        return (ordinalToValueMap != null);
    }

    /**
     * Returns the smallest ordinal defined by this enumeration.
     */
    public final int getMin() {
        return min;
    }

    /**
     * Returns the largest ordinal defined by this enumeration.
     */
    public final int getMax() {
        return max;
    }

    /**
     * Returns whether <code>ordinal</code> is valid for this enumeration.
     * This method is particularly useful in pre- and post-conditions, for
     * example
     * <blockquote>
     * <pre>&#64;param axisCode Axis code, must be a {&#64;link AxisCode} value
     * &#64;pre AxisCode.instance.isValid(axisCode)</pre>
     * </blockquote>
     *
     * @param ordinal Suspected ordinal from this enumeration.
     * @return Whether <code>ordinal</code> is valid.
     */
    public final boolean isValid(int ordinal) {
        if ((ordinal < min) || (ordinal > max)) {
            return false;
        }
        if (getName(ordinal) == null) {
            return false;
        }
        return true;
    }

    /**
     * Returns the name associated with an ordinal; the return value
     * is null if the ordinal is not a member of the enumeration.
     *
     * @pre isImmutable()
     */
    public final V getValue(int ordinal) {
        Util.assertPrecondition(isImmutable());

        return (V) ordinalToValueMap[ordinal - min];
    }

    /**
     * Returns the name associated with an ordinal; the return value
     * is null if the ordinal is not a member of the enumeration.
     *
     * @pre isImmutable()
     */
    public final String getName(int ordinal) {
        Util.assertPrecondition(isImmutable());

        final Value value = ordinalToValueMap[ordinal - min];
        return (value == null) ? null : value.getName();
    }

    /**
     * Returns the description associated with an ordinal; the return value
     * is null if the ordinal is not a member of the enumeration.
     *
     * @pre isImmutable()
     */
    public final String getDescription(int ordinal)
    {
        Util.assertPrecondition(isImmutable());

        final Value value = ordinalToValueMap[ordinal - min];
        return (value == null) ? null : value.getDescription();
    }

    /**
     * Returns the ordinal associated with a name
     *
     * @throws Error if the name is not a member of the enumeration
     */
    public final int getOrdinal(String name) {
        return getValue(name, true).getOrdinal();
    }

    /**
     * Returns the value associated with a name.
     *
     * @param name Name of enumerated value
     * @param fail Whether to throw if not found
     * @throws Error if the name is not a member of the enumeration and
     *       <code>fail</code> is true
     */
    public V getValue(String name, final boolean fail) {
        final V value = valuesByName.get(name);
        if (value == null && fail) {
            throw new Error("Unknown enum name:  " + name);
        }
        return value;
    }

    /**
     * Returns the names in this enumeration, in declaration order.
     */
    public String[] getNames() {
        return valuesByName.keySet().toArray(emptyStringArray);
    }

    /**
     * Returns the members of this enumeration, sorted by name.
     */
    public List<V> getValuesSortedByName() {
        List<V> list = new ArrayList<V>();
        final String[] names = getNames();
        Arrays.sort(names);
        for (String name : names) {
            list.add(getValue(name, true));
        }
        return list;
    }

    /**
     * Returns an error indicating that the value is illegal. (The client needs
     * to throw the error.)
     */
    public RuntimeException badValue(int ordinal) {
        return Util.newInternal(
            "bad value " + ordinal + "("
            + getName(ordinal) + ") for enumeration '"
            + getClass().getName() + "'");
    }

    /**
     * Returns an exception indicating that we didn't expect to find this value
     * here.
     */
    public RuntimeException unexpected(V value) {
        return Util.newInternal(
            "Was not expecting value '" + value
            + "' for enumeration '" + getClass().getName()
            + "' in this context");
    }

    /**
     * A <code>Value</code> represents a member of an enumerated type. If an
     * enumerated type is not based upon an explicit array of values, an
     * array of {@link BasicValue}s will implicitly be created.
     */
    public interface Value {
        String getName();
        int getOrdinal();
        String getDescription();
    }

    /**
     * <code>BasicValue</code> is an obvious implementation of {@link
     * EnumeratedValues.Value}.
     */
    public static class BasicValue implements Value {
        public final String name;
        public final int ordinal;
        public final String description;

        /**
         * @pre name != null
         */
        public BasicValue(String name, int ordinal, String description) {
            Util.assertPrecondition(name != null, "name != null");
            this.name = name;
            this.ordinal = ordinal;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public int getOrdinal() {
            return ordinal;
        }

        public String getDescription() {
            return description;
        }

        /**
         * Returns the value's name.
         */
        public String toString() {
            return name;
        }

        /**
         * Returns whether this value is equal to a given string.
         *
         * @deprecated I bet you meant to write
         *   <code>value.name_.equals(s)</code> rather than
         *   <code>value.equals(s)</code>, didn't you?
         */
        public boolean equals(String s) {
            return super.equals(s);
        }

        /**
         * Returns an error indicating that we did not expect to find this
         * value in this context. Typical use is in a <code>switch</code>
         * statement:
         *
         * <blockquote><pre>
         * switch (fruit) {
         * case Fruit.AppleORDINAL:
         *     return 1;
         * case Fruir.OrangeORDINAL:
         *     return 2;
         * default:
         *     throw fruit.unexpected();
         * }</pre></blockquote>
         */
        public RuntimeException unexpected() {
            return Util.newInternal(
                "Value " + name + " of class "
                + getClass() + " unexpected here");
        }
    }

}

// End EnumeratedValues.java
