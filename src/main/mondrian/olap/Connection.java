/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2000-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 February, 2000
*/

package mondrian.olap;
import mondrian.olap.MondrianDef;
import java.util.Locale;

/**
 * Connection to a multi-dimensional database.
 *
 * @see DriverManager
 **/
public interface Connection {
	String getConnectString();
	String getCatalogName();
	Schema getSchema();

	/**
	 * Closes this <code>Connection</code>. You may not use this
	 * <code>Connection</code> after closing it.
	 **/
	void close();

	/**
	 * Executes a query.
	 */
	Result execute(Query query);

	/**
	 * Returns the locale this connection belongs to.  Determines, for example,
	 * the currency string used in formatting cell values.
	 *
	 * @see mondrian.util.Format
	 **/
	Locale getLocale();

	/**
	 * Parses an expresion.
	 **/
	Exp parseExpression(String s);

	/**
	 * Parses a query.
	 **/
	Query parseQuery(String s);

	/**
	 * Sets the privileges for the this connection.
	 *
	 * @pre role != null
	 * @pre role.isMutable()
	 */
	void setRole(Role role);

	/**
	 * Returns the access-control profile for this connection.
	 * @post role != null
	 * @post role.isMutable()
	 */
	Role getRole();
	/**
	 * Returns a schema reader with access control appropriate to the current
	 * role.
	 */
	SchemaReader getSchemaReader();
}

// End Connection.java
