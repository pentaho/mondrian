/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi.impl;

import mondrian.spi.AccessController;

import java.util.*;

/** Trivial implementation of {@link AccessController} that gives every user
 * access to everything. */
public class TrivialAccessController implements AccessController {
    public List<String> getAccessibleRoles(Object user) {
        // "null" means no restriction on roles that can be adopted
        return null;
    }

    public List<String> getDefaultRoles(Object user) {
        // List with one null means adopt the root role ("system")
        return Collections.singletonList(null);
    }
}

// End TrivialAccessController.java
