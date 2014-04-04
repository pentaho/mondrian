/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (c) 2002-2014 Pentaho Corporation..  All rights reserved.
*/
package mondrian.spi.impl;

import mondrian.rolap.SqlStatement;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link mondrian.spi.Dialect} for the Vertica database.
 *
 * @author Pedro Alves
 * @since Sept 11, 2009
 */
public class VerticaDialect extends JdbcDialectImpl {

    public static final JdbcDialectFactory FACTORY =
        new JdbcDialectFactory(
            VerticaDialect.class,
            DatabaseProduct.VERTICA);

    /**
     * Creates a VerticaDialect.
     *
     * @param connection Connection
     */
    public VerticaDialect(Connection connection) throws SQLException {
        super(connection);
    }

    public boolean requiresAliasForFromQuery() {
        return true;
    }

    public boolean allowsFromQuery() {
        return true;
    }

    @Override
    public DatabaseProduct getDatabaseProduct() {
        return DatabaseProduct.VERTICA;
    }

    @Override
    public boolean allowsMultipleCountDistinct() {
        return false;
    }

    @Override
    public boolean allowsCountDistinctWithOtherAggs() {
      return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) {
        return false;
    }

    public String generateInline(
        List<String> columnNames,
        List<String> columnTypes,
        List<String[]> valueList)
    {
        return generateInlineGeneric(
            columnNames, columnTypes, valueList, null, false);
    }

    private static final Map<Integer, SqlStatement.Type> VERTICA_TYPE_MAP;
    static {
        Map<Integer, SqlStatement.Type> typeMapInitial =
            new HashMap<Integer, SqlStatement.Type>();
        typeMapInitial.put(Types.SMALLINT, SqlStatement.Type.LONG);
        typeMapInitial.put(Types.TINYINT, SqlStatement.Type.LONG);
        typeMapInitial.put(Types.INTEGER, SqlStatement.Type.LONG);
        typeMapInitial.put(Types.BOOLEAN, SqlStatement.Type.INT);
        typeMapInitial.put(Types.DOUBLE, SqlStatement.Type.DOUBLE);
        typeMapInitial.put(Types.FLOAT, SqlStatement.Type.DOUBLE);
        typeMapInitial.put(Types.BIGINT, SqlStatement.Type.LONG);
        VERTICA_TYPE_MAP = Collections.unmodifiableMap(typeMapInitial);
    }

    @Override
    public SqlStatement.Type getType(
        ResultSetMetaData metaData, int columnIndex)
        throws SQLException
    {
        final int columnType = metaData.getColumnType(columnIndex + 1);

        SqlStatement.Type internalType = null;
        // all int types in vertica are longs.
        if (columnType == Types.NUMERIC || columnType == Types.DECIMAL) {
            final int precision = metaData.getPrecision(columnIndex + 1);
            final int scale = metaData.getScale(columnIndex + 1);
            if (scale == 0 && precision <= 9) {
                // An int (up to 2^31 = 2.1B) can hold any NUMBER(10, 0) value
                // (up to 10^9 = 1B).
                internalType = SqlStatement.Type.INT;
            } else if (scale == 0 && precision <= 19) {
                // An int (up to 2^31 = 2.1B) can hold any NUMBER(10, 0) value
                // (up to 10^9 = 1B).
                internalType = SqlStatement.Type.LONG;
            } else {
                internalType = SqlStatement.Type.DOUBLE;
            }
        } else {
            internalType = VERTICA_TYPE_MAP.get(columnType);
            if (internalType == null) {
                internalType = SqlStatement.Type.OBJECT;
            }
        }
        logTypeInfo(metaData, columnIndex, internalType);
        return internalType;
    }

    @Override
    public boolean supportsMultiValueInExpr() {
        return true;
    }
}

// End VerticaDialect.java
