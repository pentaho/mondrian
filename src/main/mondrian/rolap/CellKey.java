/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;

/**
 * todo:
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 **/
public class CellKey
{
	public int[] ordinals;
	public CellKey(int[] ordinals)
	{
		this.ordinals = ordinals;
	}
	public boolean equals(Object o)
	{
		if (o instanceof CellKey) {
			CellKey other = (CellKey) o;
			if (other.ordinals.length != this.ordinals.length) {
				return false;
			}
			for (int i = 0; i < ordinals.length; i++) {
				if (other.ordinals[i] != this.ordinals[i]) {
					return false;
				}
			}
			return true;
		} else {
			return false;
		}
	}
	public int hashCode()
	{
		int h = 0;
		for (int i = 0; i < ordinals.length; i++) {
			h = (h * 37) ^ ordinals[i];
		}
		return h;
	}
	CellKey copy()
	{
		return new CellKey((int[])ordinals.clone());
	}
};



// End CellKey.java
