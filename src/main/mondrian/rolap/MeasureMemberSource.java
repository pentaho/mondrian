/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 21 December, 2001
*/

package mondrian.rolap;

import mondrian.rolap.sql.SqlQuery;

/**
 * A <code>MeasureMemberSource</code> implements the {@link MemberReader}
 * interface for the special Measures dimension. The {@link #qualifyQuery}
 * method usually adds filters, but this implementation adds columns to the
 * select list.
 *
 * @author jhyde
 * @since 21 December, 2001
 * @version $Id$
 **/
class MeasureMemberSource extends ArrayMemberSource
{
	MeasureMemberSource(RolapHierarchy hierarchy, RolapMember[] members)
	{
		super(hierarchy, members);
	}

	// implement MemberReader
	public void qualifyQuery(
		SqlQuery sqlQuery, RolapMember member)
	{
		RolapStoredMeasure measure = (RolapStoredMeasure) member;
		RolapCube cube = (RolapCube) member.getCube();
		cube.addToFrom(sqlQuery);
		sqlQuery.addSelect(
			measure.aggregator + "(" +
			sqlQuery.quoteIdentifier(
				cube.getAlias(), measure.column) +
			")");
	}
};

// End MeasureMemberSource.java
