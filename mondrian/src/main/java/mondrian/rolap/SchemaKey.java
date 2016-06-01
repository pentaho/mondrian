/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.util.Pair;

/**
 * Key for an instance of a schema. Schemas are equivalent if they have
 * equivalent metadata content and underlying SQL database connection.
 * Equivalent schemas can share the same cache, including a distributed
 * cache.
 */
class SchemaKey
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
