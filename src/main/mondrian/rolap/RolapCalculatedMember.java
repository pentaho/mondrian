/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 26 August, 2001
*/

package mondrian.rolap;
import mondrian.olap.*;

/**
 * todo:
 *
 * @author jhyde
 * @since 26 August, 2001
 * @version $Id$
 **/
public class RolapCalculatedMember extends RolapMember {
	private Formula formula;

	static final String[] FORMAT_PROPERTIES = {
		"format", "format_string", "FORMAT", "FORMAT_STRING"
	};

	RolapCalculatedMember(
		RolapMember parentMember, RolapLevel level, String name,
		Formula formula)
	{
		super(parentMember, level, name);
		this.formula = formula;
		Exp formatExp = null;
		for (int i = 0; i < FORMAT_PROPERTIES.length; i++) {
			formatExp = formula.getMemberProperty(FORMAT_PROPERTIES[i]);
			if (formatExp != null) {
				break;
			}
		}
		if (formatExp == null) {
			formatExp = Literal.emptyString;
		}
		setProperty(PROPERTY_FORMAT_EXP, formatExp);
	}

	// override RolapMember
	int getSolveOrder()
	{
		return 0;
	}

	Exp getExpression() {
		return formula.getExpression();
	}
}


// End RolapCalculatedMember.java
