/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.test;

import mondrian.olap.Util;
import mondrian.rolap.sql.SqlQuery;

import java.util.Set;

/**
 * Pattern for a SQL statement (or fragment thereof) expected to be produced
 * during the course of running a test.
 *
 * <p>A pattern contains a dialect. This allows a test to run against different
 * dialects.
 *
 * @see mondrian.rolap.sql.SqlQuery.Dialect
 *
 * @version $Id$
 * @author jhyde
*/
public class SqlPattern {
    private final String sql;
    private final String triggerSql;
    private final Set<Dialect> dialects;

    /**
     * Creates a pattern which applies to a collection of dialects
     * and is triggered by the first N characters of the expected statement.
     *
     * @param dialects Set of dialects
     * @param sql SQL statement
     * @param startsWithLen Length of prefix of statement to consider
     */
    public SqlPattern(
        Set<Dialect> dialects,
        String sql,
        int startsWithLen)
    {
        this(dialects, sql, sql.substring(0, startsWithLen));
    }

    /**
     * Creates a pattern which applies to one or more dialects
     * and is triggered by the first N characters of the expected statement.
     *
     * @param dialect Dialect
     * @param sql SQL statement
     * @param startsWithLen Length of prefix of statement to consider
     */
    public SqlPattern(
        Dialect dialect,
        final String sql,
        final int startsWithLen)
    {
        this(dialect, sql, sql.substring(0, startsWithLen));
    }

    /**
     * Creates a pattern which applies to one or more dialects.
     *
     * @param dialect Dialect
     * @param sql SQL statement
     * @param triggerSql Prefix of SQL statement which triggers a match; null
     *                   means whole statement
     */
    public SqlPattern(
        Dialect dialect,
         final String sql,
         final String triggerSql)
    {
        this(Util.enumSetOf(dialect), sql, triggerSql);
    }

    /**
     * Creates a pattern which applies a collection of dialects.
     *
     * @param dialects Set of dialects
     * @param sql SQL statement
     * @param triggerSql Prefix of SQL statement which triggers a match; null
     *                   means whole statement
     */
    public SqlPattern(
        Set<Dialect> dialects,
        String sql,
        String triggerSql)
    {
        this.dialects = dialects;
        this.sql = sql;
        this.triggerSql = triggerSql != null ? triggerSql : sql;
    }

    public static SqlPattern getPattern(Dialect d, SqlPattern[] patterns) {
        if (d == Dialect.UNKNOWN) {
            return null;
        }
        for (SqlPattern pattern : patterns) {
            if (pattern.hasDialect(d)) {
                return pattern;
            }
        }
        return null;
    }

    public boolean hasDialect(Dialect d) {
        return dialects.contains(d);
    }

    public String getSql() {
        return sql;
    }

    public String getTriggerSql() {
        return triggerSql;
    }

    /**
     * SQL dialect definition.
     */
    public enum Dialect {
        UNKNOWN,
        ACCESS,
        DERBY,
        CLOUDSCAPE,
        DB2,
        AS400,
        OLD_AS400,
        INFORMIX,
        MS_SQL,
        ORACLE,
        POSTGRES,
        MYSQL,
        SYBASE,
        LUCIDDB;

        public static Dialect get(SqlQuery.Dialect dialect) {
            if (dialect.isAccess()) {
                return ACCESS;
            } else if (dialect.isDerby()) {
                return DERBY;
            } else if (dialect.isCloudscape()) {
                return CLOUDSCAPE;
            } else if (dialect.isDB2()) {
                return DB2;
            } else if (dialect.isAS400()) {
                return AS400;
            } else if (dialect.isOldAS400()) {
                return OLD_AS400;
            } else if (dialect.isInformix()) {
                return INFORMIX;
            } else if (dialect.isMSSQL()) {
                return MS_SQL;
            } else if (dialect.isOracle()) {
                return ORACLE;
            } else if (dialect.isPostgres()) {
                return POSTGRES;
            } else if (dialect.isMySQL()) {
                return MYSQL;
            } else if (dialect.isSybase()) {
                return SYBASE;
            } else if (dialect.isLucidDB()) {
                return LUCIDDB;
            } else {
                return UNKNOWN;
            }
        }
    }
}

// End SqlPattern.java
