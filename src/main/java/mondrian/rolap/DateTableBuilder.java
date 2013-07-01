/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianException;
import mondrian.olap.Util;
import mondrian.server.Execution;
import mondrian.server.Locus;
import mondrian.spi.Dialect;

import java.sql.*;
import java.sql.Date;
import java.util.*;

import javax.sql.DataSource;

/**
 * Tests for, and if necessary creates and populates, a date dimension table.
 *
 * @author jhyde
*/
public class DateTableBuilder implements RolapSchema.PhysTable.Hook {
    private final Map<String, TimeColumnRole.Struct> columnRoleMap;
    private final List<MondrianDef.RealOrCalcColumnDef> xmlColumnDefs;
    private final Date startDate;
    private final Date endDate;

    DateTableBuilder(
        Map<String, TimeColumnRole.Struct> columnRoleMap,
        List<MondrianDef.RealOrCalcColumnDef> xmlColumnDefs,
        Date startDate,
        Date endDate)
    {
        this.columnRoleMap = columnRoleMap;
        this.xmlColumnDefs = xmlColumnDefs;
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public boolean apply(
        RolapSchema.PhysTable table,
        RolapConnection connection)
    {
        boolean tableExists = tableExists(connection, table);
        if (tableExists) {
            return false;
        }

        final DataSource dataSource = connection.getDataSource();
        final Dialect dialect = connection.getSchema().getDialect();
        String ddl = generateDdl(table, dialect);
        createTable(dataSource, ddl);
        String insert =
            generateInsert(
                table.schemaName, table.name, dialect, xmlColumnDefs.size());
        populateTable(table, dataSource, insert);
        return true;
    }

    private String generateInsert(
        String schemaName, String name, Dialect dialect, int columnCount)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("INSERT INTO ");
        dialect.quoteIdentifier(buf, schemaName, name);
        buf.append(" VALUES (");
        for (int i = 0; i < columnCount; i++) {
            if (i > 0) {
                buf.append(", ");
            }
            buf.append("?");
        }
        buf.append(")");
        return buf.toString();
    }

    private void createTable(DataSource dataSource, String ddl) {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate(ddl);
        } catch (SQLException e) {
            throw Util.newError(
                e,
                "Error while creating date dimension table; DDL=[" + ddl + "]");
        } finally {
            Util.close(null, statement, connection);
        }
    }

    private void populateTable(
        RolapSchema.PhysTable table,
        DataSource dataSource,
        String insert)
    {
        Connection connection = null;
        PreparedStatement pstmt = null;
        try {
            connection = dataSource.getConnection();
            pstmt = connection.prepareStatement(insert);
            List<TimeColumnRole.Struct> roles =
                new ArrayList<TimeColumnRole.Struct>();
            for (MondrianDef.RealOrCalcColumnDef xmlColumnDef : xmlColumnDefs) {
                roles.add(columnRoleMap.get(xmlColumnDef.name));
            }
            populate(roles, pstmt, startDate, endDate, Locale.getDefault());
        } catch (SQLException e) {
            throw Util.newError(
                e,
                "Error while creating date dimension table; DDL=[" + insert
                + "]");
        } finally {
            Util.close(null, pstmt, connection);
        }
    }

    public static void populate(
        List<TimeColumnRole.Struct> roles,
        PreparedStatement pstmt,
        Date startDate,
        Date endDate,
        Locale locale)
        throws SQLException
    {
        Object[] states = new Object[roles.size() + 1];
        for (int j = 0; j < roles.size(); j++) {
            states[j + 1] = roles.get(j).initialize(locale);
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        while (calendar.getTime().compareTo(endDate) < 0) {
            for (int i = 0, rolesSize = roles.size(); i < rolesSize; i++) {
                TimeColumnRole.Struct role = roles.get(i);
                role.bind(states, i + 1, calendar, pstmt);
            }
            pstmt.execute();
            calendar.add(Calendar.DATE, 1);
        }
    }

    private String generateDdl(
        RolapSchema.PhysTable table, Dialect dialect)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("CREATE TABLE ");
        dialect.quoteIdentifier(buf, table.schemaName, table.name);
        buf.append(" (\n");
        int i = 0;
        RolapSchema.PhysKey primaryKey = table.keysByName.get("primary");
        String singletonKey;
        if (primaryKey != null
            && primaryKey.columnList.size() == 1)
        {
            singletonKey = primaryKey.columnList.get(0).name;
        } else {
            singletonKey = null;
        }
        for (MondrianDef.RealOrCalcColumnDef xmlColumnDef : xmlColumnDefs) {
            if (i++ > 0) {
                buf.append(",\n");
            }
            buf.append("    ");
            dialect.quoteIdentifier(buf, xmlColumnDef.name);
            buf.append(" ");
            buf.append(
                dialect.datatypeToString(
                    RolapSchemaLoader.toType(xmlColumnDef.type),
                    20, 0));
            buf.append(" NOT NULL");
            if (xmlColumnDef.name.equals(singletonKey)) {
                buf.append(" PRIMARY KEY");
            }
        }
        buf.append(")");
        return buf.toString();
    }

    private boolean tableExists(
        RolapConnection connection,
        RolapSchema.PhysTable table)
    {
        final DataSource dataSource = connection.getDataSource();
        final Dialect dialect = connection.getSchema().getDialect();

        StringBuilder buf = new StringBuilder();
        buf.append("select count(*) from ");
        dialect.quoteIdentifier(buf, table.schemaName, table.name);
        int rowCount;
        SqlStatement sqlStatement = null;
        try {
            sqlStatement = RolapUtil.executeQuery(
                dataSource,
                buf.toString(),
                new Locus(
                    new Execution(connection.getInternalStatement(), 0),
                    "Auto-create date table: existence check",
                    null));
            ResultSet resultSet = sqlStatement.getResultSet();
            if (resultSet.next()) {
                rowCount = resultSet.getInt(1);
            }
            resultSet.close();
            return true;
        } catch (SQLException e) {
            throw Util.newError(e, "While validating auto-create table");
        } catch (MondrianException e) {
            if (e.getCause() instanceof SQLException) {
                // There was an error. Assume that this is because the
                // table does not exist.
                return false;
            }
            throw e;
        } finally {
            if (sqlStatement != null) {
                sqlStatement.close();
            }
        }
    }
}

// End DateTableBuilder.java
