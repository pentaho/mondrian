/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 21, 2002
*/

package mondrian.rolap.sql;


class SqlJoin extends SqlRelation
{
	SqlRelation left, right;
	String type, on;
	SqlJoin(
		SqlQuery query, SqlRelation left, String type, SqlRelation right,
		String on, String alias)
	{
		super(query);
		this.left = left;
		this.type = type;
		this.right = right;
		this.on = on;
		this.alias = alias;
	}
	public String toString()
	{
		return "(" + left.toString() + " " + type + " join " +
			right.toString() + " on " + on + ") as " +
			query.quoteIdentifier(alias);
	}
	SqlRelation find(String alias)
	{
		SqlRelation rel = left.find(alias);
		if (rel != null) {
			return rel;
		}
		return right.find(alias);
	}
}

// End SqlJoin.java