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


abstract class SqlRelation
{
	protected SqlQuery query;
	SqlRelation(SqlQuery query) {
		this.query = query;
	}
	String alias;
	abstract SqlRelation find(String alias);
}

// End SqlRelation.java
