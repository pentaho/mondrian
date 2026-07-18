/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



package mondrian.rolap.sql;

/**
 * Runs a SQL query.
 *
 * <p>Useful for testing purposes.
 *
 * @author jhyde
 * @since 30 August, 2001
 */
public interface SqlQueryChecker {
    void onGenerate(SqlQuery q);
}

// End SqlQueryChecker.java
