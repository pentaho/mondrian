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
import mondrian.olap.Evaluator;
import java.util.Hashtable;

/**
 * A <code>CachingCellReader</code> implements a simplistic caching
 * scheme. The first time you ask for a cell, it goes to the underlying
 * reader. If you ever ask for it again, it remembers the value.
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 **/
class CachingCellReader implements CellReader
{
	RolapCube cube;
	/** Underlying reader. **/
	CellReader cellReader;
	/** Workspace. */
	CellKey key;
	Hashtable mapPositionToValue;

	CachingCellReader(RolapCube cube, CellReader cellReader)
	{
		this.cube = cube;
		this.cellReader = cellReader;
		this.key = new CellKey(new int[cube.getDimensions().length]);
		this.mapPositionToValue = new Hashtable();
	}

	// implement CellReader
	public synchronized Object get(Evaluator evaluator)
	{
		for (int i = 0; i < key.ordinals.length; i++) {
			RolapMember member = (RolapMember)
				evaluator.getContext(cube.getDimensions()[i]);
			key.ordinals[i] = member.ordinal;
		}
		if (mapPositionToValue.containsKey(key)) {
			return mapPositionToValue.get(key);
		} else {
			Object o = cellReader.get(evaluator);
			mapPositionToValue.put(key.copy(), o);
			return o;
		}
	}
};

// End CachingCellReader.java
