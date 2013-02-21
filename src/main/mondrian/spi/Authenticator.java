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

import mondrian.olap.*;

import javax.naming.AuthenticationException;

/** Callback that authenticates. */
public interface Authenticator {
    /**
     * Authenticates a potential connection.
     *
     * <p>Called from
     * {@link MondrianServer#authenticate(mondrian.olap.Util.PropertyList)}</p>.
     *
     * <p>Returns a object if authentication is successful. Throws
     * {@link javax.naming.AuthenticationException} if not successful.
     * Never returns null.</p>
     *
     * <p>A container will typically arrange that the returned object is of a
     * type meaningful to the implementation of {@link AccessController}.</p>
     *
     * @param list List of connection properties, derived by parsing the
     *             connect string of a potential connection, and adding
     *             "user" and "password" properties, if supplied
     *
     * @return Object containing information about the user
     */
    Object authenticate(Util.PropertyList list) throws AuthenticationException;
}

// End Authenticator.java
