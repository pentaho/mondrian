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

	/**
	 * Closes this <code>MdxConnection</code>. You may not use this
	 * <code>MdxConnection</code> after closing it.
	 **/
	void close();

	/**
	 * Find a cube called <code>cube</code> in the current catalog; if no cube
	 * exists, <code>failIfNotFound</code> controls whether to raise an error
	 * or return null.
	 **/
	Cube lookupCube(String cube,boolean failIfNotFound);

	/**
	 * Find the names of all cubes in a given database.
	 **/
	String[] listCubeNames();

	void loadSchema(MondrianDef.Schema xmlSchema);

	Result execute(Query query);

	/**
	 * Returns the locale this connection belongs to.  Determines, for example,
	 * the curreny string used in formatting cell values.
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
}


// End Connection.java
