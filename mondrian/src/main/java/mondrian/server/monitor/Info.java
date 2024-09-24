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

package mondrian.server.monitor;

import mondrian.util.BeanMap;

/**
 * Abstract base class for objects returned from polling the
 * {@link mondrian.server.monitor.Monitor} for server state.
 */
abstract class Info {

    /**
     * A printout of the stack trace which represents the code stack
     * when the event was created. Useful for debugging purposes and
     * identifying orphaned connections and statements.
     */
    public final String stack;

    protected Info(String stack) {
        this.stack = stack;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + new BeanMap(this);
    }
}

// End Info.java
