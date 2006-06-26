/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap;

import mondrian.rolap.RolapSchema;

/**
 * Implementation of {@link MondrianServer}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 25, 2006
 */
class MondrianServerImpl extends MondrianServer {
    public void flushSchemaCache() {
        RolapSchema.clearCache();
    }

    public void flushDataCache() {
        // not implemented
    }
}

// End MondrianServerImpl.java
