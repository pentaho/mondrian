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
import mondrian.olap.MondrianDef;

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
	MondrianDef.Expression expression;
	/** For SQL generator. Has values "SUM", "COUNT", etc. */
	String aggregator;
	private RolapCube cube;

	RolapStoredMeasure(
			RolapCube cube, RolapMember parentMember, RolapLevel level, String name,
			String formatString, MondrianDef.Expression expression,
			String aggregator) {
		super(parentMember, level, name, formatString);
		this.cube = cube;
		this.expression = expression;
		Util.assertTrue(aggregator.equals("sum") || aggregator.equals("count"));
		this.aggregator = aggregator;
	}

	RolapStoredMeasure(
			RolapCube cube, RolapMember parentMember, RolapLevel level,
			String name, String formatString, String column, String aggregator) {
		this(
				cube, parentMember, level, name, formatString,
				new MondrianDef.Column(cube.fact.getAlias(), column),
				aggregator);
	}

	// implement RolapMeasure
	CellReader getCellReader() {
		return cube.cellReader;
	}
}

// End RolapStoredMeasure.java
