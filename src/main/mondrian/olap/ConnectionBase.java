/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 6 August, 2001
*/

package mondrian.olap;

/**
 * todo:
 *
 * @author jhyde
 * @since 6 August, 2001
 * @version $Id$
 **/
public abstract class ConnectionBase implements Connection
{
	protected Role role;

	public String getFullConnectString()
	{
		String s = getConnectString(),
			catalogName = getCatalogName();
		if (catalogName != null) {
			if (!s.endsWith(";")) {
				s += ";";
			}
			s += "Initial Catalog=" + catalogName + ";";
		}
		return s;
	}

	public Query parseQuery(String s) {
		try {
			boolean debug = false;
			Parser parser = new Parser();
			Query q = (Query) parser.parseInternal(this, s, debug);
			return q;
		} catch (Throwable e) {
			throw Util.newError(e, "Failed to parse query [" + s + "]");
		}
	}

	public Exp parseExpression(String s) {
		Util.assertTrue(
				s.startsWith("'") && s.endsWith("'"),
				"only string literals are supported right now");
		boolean symbol = false;
		String s2 = s.substring(1, s.length() - 1);
		return Literal.createString(s2);
	}

	public void setRole(Role role) {
		this.role = role;
	}
}


// End ConnectionBase.java
