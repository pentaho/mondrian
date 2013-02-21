/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.spi;

import java.util.List;

/**
 * Strategy that determines what roles a user can see.
 *
 * <p>The object passed to {@link #getAccessibleRoles(Object)} and
 * {@link #getDefaultRoles(Object)} is the one returned from
 * {@link Authenticator#authenticate(mondrian.olap.Util.PropertyList)}.
 * The container should use compatible implementations of
 * {@link Authenticator} and {@code AccessController}.</p>
 */
public interface AccessController {
    /** Returns a list of role names this user is allowed to adopt in the
     * current schema. null means no restriction. */
    List<String> getAccessibleRoles(Object user);

    /** Returns a list of role names that this user will use if they do not
     * specify explicitly. */
    List<String> getDefaultRoles(Object user);
}

// End AccessController.java
