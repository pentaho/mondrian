/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.olap;
import java.util.HashMap;
import java.util.Iterator;

/**
 * <code>EnumeratedValues</code> is a helper class for declaring a set of
 * symbolic constants which have both names and possibly non-contiguous
 * ordinals associated with them.
 *
 * <p>Typical usage (e.g. SQLTypes) is to define a class which declares a set
 * of constant values in the standard way, along with a static
 * <code>EnumeratedValues</code> member named enum.  The class static
 * initializer block should call enum.putName for each value, and then
 * enum.makeImmutable().  Users of the class can then map names to ordinals and
 * back via getEnum(), but may not modify enum, because it is initialized as
 * immutable.</p>
 **/
public class EnumeratedValues implements Cloneable
{
	/** map symbol names to ordinal values */
	private HashMap nameToOrdinalMap;

	/** the smallest ordinal value */
	private int min;
	
	/** the largest ordinal value */
	private int max;

	// the variables below are only set AFTER makeImmutable() has been called

	/** an array mapping ordinal values to names; it is biased by the
	 * min value */
	private String [] ordinalToNameMap;

	/**
	 * Creates a new empty, mutable enumeration.
	 */
	public EnumeratedValues()
	{
		nameToOrdinalMap = new HashMap();
		this.min = Integer.MAX_VALUE;
		this.max = Integer.MIN_VALUE;
	}

	/** Creates an enumeration, initialize it with an array of strings, and
	 * freezes it. */
	public EnumeratedValues(String[] names)
	{
		this();
		for (int i = 0; i < names.length; i++) {
			putName(i, names[i]);
		}
		makeImmutable();
	}

	/** Create an enumeration, initializes it with arrays of code/name pairs,
	 * and freezes it. */
	public EnumeratedValues(String[] names, int[] codes)
	{
		this();
		for (int i = 0; i < names.length; i++) {
			putName(codes[i], names[i]);
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
	public void putName(int ordinal,String name)
	{
		Util.assertPrecondition(!isImmutable());
		nameToOrdinalMap.put(name,new Integer(ordinal));
		min = Math.min(min,ordinal);
		max = Math.max(max,ordinal);
	}

	/**
	 * Freezes the enumeration, preventing it from being further modified.
	 */
	public void makeImmutable()
	{
		ordinalToNameMap = new String[1+max-min];
		for (Iterator names = nameToOrdinalMap.keySet().iterator();
                names.hasNext(); ) {
			String name = (String) names.next();
			int ordinal = getOrdinal(name);
			ordinalToNameMap[ordinal-min] = name;
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

	public final boolean isOrdinalValid(int ordinal)
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
		return ordinalToNameMap[ordinal-min];
	}

	/**
	 * Returns the ordinal associated with a name
     *
     * @throws {@link Error} if the name is not a member of the enumeration
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

}

// End EnumeratedValues.java
