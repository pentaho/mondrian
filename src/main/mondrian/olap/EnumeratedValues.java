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
import java.util.Hashtable;
import java.util.Enumeration;

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
	private Hashtable nameToOrdinalMap;

	/** the smallest ordinal value */
	private int min;
	
	/** the largest ordinal value */
	private int max;

	// the variables below are only set AFTER makeImmutable() has been called

	/** an array mapping ordinal values to names; it is biased by the
	 * min value */
	private String [] ordinalToNameMap;

	/**
	 * Create a new empty, mutable enumeration.
	 */
	public EnumeratedValues()
	{
		nameToOrdinalMap = new Hashtable();
		this.min = Integer.MAX_VALUE;
		this.max = Integer.MIN_VALUE;
	}

	/** Create an enumeration, initialize it with an array of strings, and
	 * freeze it. */
	public EnumeratedValues(String[] names)
	{
		this();
		for (int i = 0; i < names.length; i++) {
			putName(i, names[i]);
		}
		makeImmutable();
	}

	/** Create an enumeration, initialize it with arrays of code/name pairs,
	 * and freeze it. */
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
		clone.nameToOrdinalMap = (Hashtable) nameToOrdinalMap.clone();
		clone.ordinalToNameMap = null;
		return clone;
	}
	
	/**
	 * Create a mutable enumeration from an existing enumeration, which may
	 * already be immutable.
	 */
	public EnumeratedValues getMutableClone()
	{
		return (EnumeratedValues) clone();
	}
	
	/**
	 * Associate a symbolic name with an ordinal value.
	 * PRECONDITION:  !isImmutable()
	 */
	public void putName(int ordinal,String name)
	{
		// IMPLEMENT assert(!isImmutable());
		nameToOrdinalMap.put(name,new Integer(ordinal));
		min = Math.min(min,ordinal);
		max = Math.max(max,ordinal);
	}

	/**
	 * Freeze the enumeration, preventing it from being further modified.
	 */
	public void makeImmutable()
	{
		ordinalToNameMap = new String[1+max-min];
		Enumeration names = nameToOrdinalMap.keys();
		while (names.hasMoreElements()) {
			String name = (String) names.nextElement();
			int ordinal = getOrdinal(name);
			ordinalToNameMap[ordinal-min] = name;
		}
	}

	public final boolean isImmutable()
	{
		return (ordinalToNameMap != null);
	}

	/**
	 * Get the smallest ordinal defined by this enumeration.
	 */
	public final int getMin()
	{
		return min;
	}

	/**
	 * Get the largest ordinal defined by this enumeration.
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
	 * Get the name associated with an ordinal; the return value
	 * is null if the ordinal is not a member of the enumeration.
	 * PRECONDITION:  isImmutable()
	 */
	public final String getName(int ordinal)
	{
		// IMPLEMENT assert(isImmutable());
		return ordinalToNameMap[ordinal-min];
	}

	/**
	 * Get the ordinal associated with a name; an Error is thrown
	 * if the name is not a member of the enumeration.
	 */
	public final int getOrdinal(String name)
	{
		Integer i = findOrdinal(name);
		if (i == null) throw new Error("Unknown enum name:  "+name);
		return i.intValue();
	}
	
	/**
	 * Get the ordinal associated with a name; the return value is
	 * null if the name is not a member of the enumeration.
	 */
	public final Integer findOrdinal(String name)
	{
		return (Integer) nameToOrdinalMap.get(name);
	}

}

// End EnumeratedValues.java
