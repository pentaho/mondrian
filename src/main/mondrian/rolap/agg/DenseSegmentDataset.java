/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 March, 2002
*/
package mondrian.rolap.agg;

/**
 * A <code>DenseSegmentDataset</code> is a means of storing segment values
 * which is suitable when most of the combinations of keys have a value
 * present.
 *
 * <p>The storage requirements are as follows. Table requires 1 word per
 * cell.</p>
 *
 * @author jhyde
 * @since 21 March, 2002
 * @version $Id$
 **/
class DenseSegmentDataset implements SegmentDataset
{
	Segment segment;
	Object[] values; // length == m[0] * ... * m[axes.length-1]

	public Object get(int[] keys)
	{
		int offset = getOffset(keys);
		return values[offset];
	}
	public double getBytes()
	{
		// assume a slot, key, and value are each 4 bytes
		return values.length * 12;
	}
	boolean contains(Object[] keys)
	{
		return getOffset(keys) >= 0;
	}
	Object get(Object[] keys)
	{
		int offset = getOffset(keys);
		return keys[offset];
	}
	void put(Object[] keys, Object value)
	{
		int offset = getOffset(keys);
		keys[offset] = value;
	}
	private int getOffset(int[] keys)
	{
		int offset = 0;
		for (int i = 0; i < keys.length; i++) {
			Aggregation.Axis axis = segment.axes[i];
			offset *= axis.keys.length;
			offset += keys[i];
		}
		return offset;
	}
	private int getOffset(Object[] keys)
	{
		int offset = 0;
outer:
		for (int i = 0; i < keys.length; i++) {
			Aggregation.Axis axis = segment.axes[i];
			offset *= axis.keys.length;
			Object value = keys[i];
			for (int j = 0, axisLength = axis.keys.length; j <
					 axisLength; j++) {
				if (axis.keys[j].equals(value)) {
					offset += j;
					continue outer;
				}
			}
			return -1; // not found
		}
		return offset;
	}
}

// End DenseSegmentDataset.java
