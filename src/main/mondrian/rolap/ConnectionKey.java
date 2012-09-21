/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.util.*;

import javax.sql.DataSource;

/**
 * Globally unique identifier for the definition of a JDBC database connection.
 *
 * <p>Two connections should have the same connection key if and only if their
 * databases have the same content.</p>
 *
 * @see RolapConnectionProperties#JdbcConnectionUuid
 *
 * @author jhyde
 */
class ConnectionKey extends StringKey {
    private ConnectionKey(String s) {
        super(s);
    }

    static ConnectionKey create(
        final String connectionUuidStr,
        final DataSource dataSource,
        final String catalogUrl,
        final String connectionKey,
        final String jdbcUser,
        final String dataSourceStr)
    {
        String s;
        if (connectionUuidStr != null
            && connectionUuidStr.length() != 0)
        {
            s = connectionUuidStr;
        } else {
            final StringBuilder buf = new StringBuilder(100);
            if (dataSource != null) {
                attributeValue(buf, "jvm", Util.JVM_INSTANCE_UUID);
                attributeValue(
                    buf, "dataSource", System.identityHashCode(dataSource));
            } else {
                attributeValue(buf, "connectionKey", connectionKey);
                attributeValue(buf, "catalogUrl", catalogUrl);
                attributeValue(buf, "jdbcUser", jdbcUser);
                attributeValue(buf, "dataSourceStr", dataSourceStr);
            }
            s = new ByteString(Util.digestMd5(buf.toString())).toString();
        }
        return new ConnectionKey(s);
    }

    static void attributeValue(
        StringBuilder buf, String attribute, Object value)
    {
        if (value == null) {
            return;
        }
        if (buf.length() > 0) {
            buf.append(';');
        }
        buf.append(attribute)
            .append('=');
        Util.quoteForMdx(buf, value.toString());
    }
}

// End ConnectionKey.java
