/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2004-2005 TONBELLER AG
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2007 Pentaho
// All Rights Reserved.
*/
package mondrian.spi;

import javax.sql.DataSource;

import mondrian.olap.Util;

/**
 * A schema key processor to allow a custom key generation for the schema pool
 */
public interface SchemaKeyProcessor {

    public String generateKey(
            final String catalogUrl,
            final String connectionKey,
            final String jdbcUser,
            final String dataSourceStr,
            final DataSource dataSource,
            final Util.PropertyList connectInfo) throws Exception;
}

// End DynamicSchemaProcessor.java

