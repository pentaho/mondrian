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
import mondrian.olap.MondrianDef;
import mondrian.olap.Evaluator;
import mondrian.olap.Literal;
import mondrian.olap.Exp;

/**
 * todo:
 *
 * @author jhyde
 * @since 10 August, 2001
 * @version $Id$
 **/
abstract class RolapMeasure extends RolapMember
{
	/** Holds the {@link mondrian.rolap.RolapStar} from which this member is computed.
	 * Untyped, because another implementation might store it somewhere else.
	 */
	Object star;

	RolapMeasure(
			RolapMember parentMember, RolapLevel level, String name,
			String formatString)
	{
		super(parentMember, level, name);
		if (formatString == null) {
			formatString = "";
		}
		Exp formatExp = level.getCube().getConnection().parseExpression(
				"'" + formatString + "'");
		setProperty(PROPERTY_FORMAT_EXP, formatExp);
	}

	static RolapMeasure create(
		RolapMeasure parentMember, RolapLevel level,
		MondrianDef.Measure xmlMeasure)
	{
		return new RolapStoredMeasure(
			parentMember, level, xmlMeasure.name, xmlMeasure.formatString,
			xmlMeasure.column, xmlMeasure.aggregator);
	}
};



// End RolapMeasure.java
