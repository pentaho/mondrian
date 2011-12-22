/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2005 Julian Hyde and others
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
