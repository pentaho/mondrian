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

/**
 * Globally unique identifier for the metadata content of a schema.
 *
 * <p>Two schemas have the same content if they have same schema XML.
 * But schemas are also deemed to have the same content if they are read from
 * the same URL (subject to rules about how often the contents of a URL change)
 * or if their content has the same MD5 hash.</p>
 *
 * @see SchemaKey
 *
 * @author jhyde
 */
class SchemaContentKey extends StringKey {
    private SchemaContentKey(String s) {
        super(s);
    }

    static SchemaContentKey create(
        final Util.PropertyList connectInfo,
        final String catalogUrl,
        final String catalogContents)
    {
        final String catalogContentProp =
            RolapConnectionProperties.CatalogContent.name();
        final String dynamicSchemaProp =
            RolapConnectionProperties.DynamicSchemaProcessor.name();

        StringBuilder buf = new StringBuilder();
        if (!Util.isEmpty(connectInfo.get(catalogContentProp))
            || !Util.isEmpty(connectInfo.get(dynamicSchemaProp)))
        {
            ConnectionKey.attributeValue(buf, "catalogStr", catalogContents);
        } else {
            ConnectionKey.attributeValue(buf, "catalog", catalogUrl);
        }
        return new SchemaContentKey(
            new ByteString(Util.digestMd5(buf.toString())).toString());
    }
}

// End SchemaContentKey.java
