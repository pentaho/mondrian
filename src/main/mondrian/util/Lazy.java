/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.util;

import mondrian.olap.Util;

/**
* Lazily initialized value.
 *
 * @author jhyde
 */
public class Lazy<T> {
    private final Util.Function0<T> factory;
    private boolean done;
    private T value;

    /**
     * Creates a lazily initialized value.
     *
     * @param factory Value factory
     */
    public Lazy(Util.Function0<T> factory) {
        assert factory != null;
        this.factory = factory;
    }

    /**
     * Returns the value. If the value has never been asked for before, gets
     * a value from the factory.
     *
     * @return Value
     */
    public synchronized T get() {
        if (!done) {
            done = true;
            value = factory.apply();
        }
        return value;
    }
}

// End Lazy.java
