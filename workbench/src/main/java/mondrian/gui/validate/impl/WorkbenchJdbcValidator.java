/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.gui.validate.impl;

import mondrian.gui.JdbcMetaData;
import mondrian.gui.validate.JdbcValidator;

import java.util.List;

/**
 * Implementation of <code>JdbcValidator</code> for Workbench.
 *
 * @author mlowery
 */
public class WorkbenchJdbcValidator implements JdbcValidator {

    private JdbcMetaData jdbcMetadata;

    public WorkbenchJdbcValidator(JdbcMetaData jdbcMetadata) {
        super();
        this.jdbcMetadata = jdbcMetadata;
    }

    public int getColumnDataType(
        String schemaName, String tableName, String colName)
    {
        return jdbcMetadata.getColumnDataType(schemaName, tableName, colName);
    }

    public boolean isColExists(
        String schemaName, String tableName, String colName)
    {
        return jdbcMetadata.isColExists(schemaName, tableName, colName);
    }

    public boolean isInitialized() {
        return jdbcMetadata.getErrMsg() == null;
    }

    public boolean isTableExists(String schemaName, String tableName) {
        return jdbcMetadata.isTableExists(schemaName, tableName);
    }

    public boolean isSchemaExists(String schemaName) {
        List<String> theSchemas = jdbcMetadata.getAllSchemas();
        return theSchemas != null && theSchemas.contains(schemaName);
    }
}

// End WorkbenchJdbcValidator.java
