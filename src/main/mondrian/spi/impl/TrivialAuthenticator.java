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

import mondrian.olap.Util;
import mondrian.spi.Authenticator;

/** Trivial implementation of {@link mondrian.spi.Authenticator} that allows
 * every user to log in. */
public class TrivialAuthenticator implements Authenticator {
    public Object authenticate(Util.PropertyList list) {
        return DummyAuthenticationResult.INSTANCE;
    }

    enum DummyAuthenticationResult {
        INSTANCE
    }
}

// End TrivialAuthenticator.java
