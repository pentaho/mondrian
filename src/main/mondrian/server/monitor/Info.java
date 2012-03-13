/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho
// All Rights Reserved.
*/
package mondrian.server.monitor;

import mondrian.util.BeanMap;

/**
 * Abstract base class for objects returned from polling the
 * {@link mondrian.server.monitor.Monitor} for server state.
 */
abstract class Info {
    protected Info() {
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + new BeanMap(this);
    }
}

// End Info.java
