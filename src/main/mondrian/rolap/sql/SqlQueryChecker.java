/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Mar 21, 2002
*/
package mondrian.rolap.sql;

/**
 * Runs a SQL query.
 *
 * <p>Useful for testing purposes.
 *
 * @author jhyde
 * @since 30 August, 2001
 * @version $Id$
 */
public interface SqlQueryChecker {
    void onGenerate(SqlQuery q);
}

// End SqlQueryChecker.java
