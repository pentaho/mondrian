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
 * <code>RolapCalculatedMember</code> is a member based upon a {@link Formula}.
 *
 * <p>It is created before the formula has been resolved; the formula is
 * responsible for setting the "format_string" property.
 *
 * @author jhyde
 * @since 26 August, 2001
 * @version $Id$
 **/
public class RolapCalculatedMember extends RolapMember {
	final Formula formula;

	RolapCalculatedMember(
			RolapMember parentMember, RolapLevel level, String name,
			Formula formula) {
		super(parentMember, level, name);
		this.formula = formula;
	}

	// override RolapMember
	int getSolveOrder() {
		return 0;
	}
}


// End RolapCalculatedMember.java
