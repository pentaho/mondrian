/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1998-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.olap;
import java.util.HashMap;
import java.util.Iterator;

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
 **/
public class EnumeratedValues implements Cloneable
{
	/** map symbol names to ordinal values */
	private HashMap nameToOrdinalMap = new HashMap();
	/** map symbol names to descriptions */
	private HashMap nameToDescriptionMap = new HashMap();

	/** the smallest ordinal value */
	private int min = Integer.MAX_VALUE;
	
	/** the largest ordinal value */
	private int max = Integer.MIN_VALUE;

	// the variables below are only set AFTER makeImmutable() has been called

	/** an array mapping ordinal values to names; it is biased by the
	 * min value */
	private String [] ordinalToNameMap;
	/** an array mapping ordinal values to desciptions; it is biased by the
	 * min value */
	private String [] ordinalToDescriptionMap;
	private static final String[] emptyStringArray = new String[0];

	/**
	 * Creates a new empty, mutable enumeration.
	 */
	public EnumeratedValues() {
	}

	/** Creates an enumeration, initialize it with an array of strings, and
	 * freezes it. */
	public EnumeratedValues(String[] names)
	{
		for (int i = 0; i < names.length; i++) {
			register(i, names[i], names[i]);
		}
		makeImmutable();
	}

	/** Create an enumeration, initializes it with arrays of code/name pairs,
	 * and freezes it. */
	public EnumeratedValues(String[] names, int[] codes)
	{
		for (int i = 0; i < names.length; i++) {
			register(codes[i], names[i], names[i]);
		}
		makeImmutable();
	}

	/** Create an enumeration, initializes it with arrays of code/name pairs,
	 * and freezes it. */
	public EnumeratedValues(String[] names, int[] codes, String[] descriptions)
	{
		for (int i = 0; i < names.length; i++) {
			register(codes[i], names[i], descriptions[i]);
		}
		makeImmutable();
	}

	protected Object clone()
	{
		EnumeratedValues clone = null;;
		try {
			clone = (EnumeratedValues) super.clone();
		} catch(CloneNotSupportedException ex) {
			// IMPLEMENT internal error?
		}
		clone.nameToOrdinalMap = (HashMap) nameToOrdinalMap.clone();
		clone.ordinalToNameMap = null;
		return clone;
	}
	
	/**
	 * Creates a mutable enumeration from an existing enumeration, which may
	 * already be immutable.
	 */
	public EnumeratedValues getMutableClone()
	{
		return (EnumeratedValues) clone();
	}
	
	/**
	 * Associates a symbolic name with an ordinal value.
     *
	 * @pre !isImmutable()
	 */
	public void register(int ordinal,String name, String description)
	{
		Util.assertPrecondition(!isImmutable());
		final Integer i = new Integer(ordinal);
		nameToOrdinalMap.put(name,i);
		nameToDescriptionMap.put(name,description);
		min = Math.min(min,ordinal);
		max = Math.max(max,ordinal);
	}

	/**
	 * Freezes the enumeration, preventing it from being further modified.
	 */
	public void makeImmutable()
	{
		ordinalToNameMap = new String[1 + max - min];
		ordinalToDescriptionMap = new String[1 + max - min];
		for (Iterator names = nameToOrdinalMap.keySet().iterator();
                names.hasNext(); ) {
			String name = (String) names.next();
			int ordinal = getOrdinal(name);
			ordinalToNameMap[ordinal - min] = name;
			String description = (String) nameToDescriptionMap.get(name);
			ordinalToDescriptionMap[ordinal - min] = description;
		}
	}

	public final boolean isImmutable()
	{
		return (ordinalToNameMap != null);
	}

	/**
	 * Returns the smallest ordinal defined by this enumeration.
	 */
	public final int getMin()
	{
		return min;
	}

	/**
	 * Returns the largest ordinal defined by this enumeration.
	 */
	public final int getMax()
	{
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
	public final boolean isValid(int ordinal)
	{
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
	public final String getName(int ordinal)
	{
		Util.assertPrecondition(isImmutable());
		return ordinalToNameMap[ordinal - min];
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
		return ordinalToDescriptionMap[ordinal - min];
	}

	/**
	 * Returns the ordinal associated with a name
     *
     * @throws Error if the name is not a member of the enumeration
	 */
	public final int getOrdinal(String name)
	{
		Integer i = findOrdinal(name);
		if (i == null) {
            throw new Error("Unknown enum name:  "+name);
        }
		return i.intValue();
	}
	
	/**
	 * Returns the ordinal associated with a name; the return value is
	 * null if the name is not a member of the enumeration.
	 */
	public final Integer findOrdinal(String name)
	{
		return (Integer) nameToOrdinalMap.get(name);
	}

	/**
	 * Returns an error indicating that the value is illegal. (The client needs
	 * to throw the error.)
	 */
	public RuntimeException badValue(int ordinal) {
		return Util.newInternal("bad value " + ordinal + "(" +
				getName(ordinal) + ") for enumeration '" +
				getClass().getName() + "'");
	}

	/**
	 * Returns the names in this enumeration, in no particular order.
	 */ 
	public String[] getNames() {
		return (String[]) nameToOrdinalMap.keySet().toArray(emptyStringArray);
	}
}

// End EnumeratedValues.java
