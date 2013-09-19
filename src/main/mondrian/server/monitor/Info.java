/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

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
