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
	Exp exp;

	RolapCalculatedMember(
		RolapMember parentMember, RolapLevel level, String name,
		Formula formula)
	{
		super(parentMember, level, name);
		this.exp = formula.getExpression();
		Exp formatExp = formula.getMemberProperty("format");
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
}


// End RolapCalculatedMember.java
