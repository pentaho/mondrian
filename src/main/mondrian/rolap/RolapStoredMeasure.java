/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2003 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 10 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.MondrianDef;
import mondrian.olap.Property;
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
	final MondrianDef.Expression expression;
	/** For SQL generator. Has values "SUM", "COUNT", etc. */
	final RolapAggregator aggregator;
	final RolapCube cube;

	RolapStoredMeasure(
			RolapCube cube, RolapMember parentMember, RolapLevel level, String name,
			String formatString, MondrianDef.Expression expression,
			String aggregatorName) {
		super(parentMember, level, name, formatString);
		this.cube = cube;
		this.expression = expression;
        this.aggregator = (RolapAggregator)
                RolapAggregator.enumeration.getValue(aggregatorName);
        if (this.aggregator == null) {
            throw Util.newError("Unknown aggregator '" + aggregatorName + "'");
        }
		setProperty(Property.PROPERTY_AGGREGATION_TYPE, aggregator);
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
