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
import mondrian.olap.Util;

/**
 * todo:
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 */
class RolapStoredMeasure extends RolapMeasure
{
	/** For SQL generator. Column which holds the value of the measure. */
	String column;
	/** For SQL generator. Has values "SUM", "COUNT", etc. */
	String aggregator;

	RolapStoredMeasure(
		RolapMember parentMember, RolapLevel level, String name,
		String formatString, String column, String aggregator)
	{
		super(parentMember, level, name, formatString);
		this.column = column;
		Util.assertTrue(aggregator.equals("sum") || aggregator.equals("count"));
		this.aggregator = aggregator;
	}

	// implement RolapMeasure
	CellReader getCellReader()
	{
		return ((RolapCube) getCube()).cellReader;
	}
};



// End RolapStoredMeasure.java
