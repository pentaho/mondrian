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


class SqlTable extends SqlRelation
{
	String name;
	SqlTable(SqlQuery query) {
		super(query);
	}
	public String toString()
	{
		return query.quoteIdentifier(name) + " as " +
			query.quoteIdentifier(alias);
	}
	SqlRelation find(String alias)
	{
		if (this.alias.equals(alias)) {
			return this;
		} else {
			return null;
		}
	}
}

// End SqlTable.java
