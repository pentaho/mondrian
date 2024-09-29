/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.rolap;

import mondrian.util.Pair;

/**
 * Key for an instance of a schema. Schemas are equivalent if they have
 * equivalent metadata content and underlying SQL database connection.
 * Equivalent schemas can share the same cache, including a distributed
 * cache.
 */
public class SchemaKey
    extends Pair<SchemaContentKey, ConnectionKey>
{
    /** Creates a schema key. */
    SchemaKey(
        SchemaContentKey schemaContentKey,
        ConnectionKey connectionKey)
    {
        super(schemaContentKey, connectionKey);
        assert schemaContentKey != null;
        assert connectionKey != null;
    }
}

// End SchemaKey.java
