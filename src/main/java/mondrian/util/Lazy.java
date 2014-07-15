/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2012-2013 Pentaho
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
    private Util.Function0<T> factory;
    private int state = STATE_INIT;
    private T value;
    private Throwable throwable;

    private static final int STATE_INIT = 0;
    private static final int STATE_SUCCESS = 1;
    private static final int STATE_RUNTIME_EXCEPTION = 2;
    private static final int STATE_ERROR = 3;

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
     * a value from the factory. If the attempt to populate the value failed
     * with an exception, throws the same exception.
     *
     * @return Value
     */
    public synchronized T get() {
        switch (state) {
        case STATE_INIT:
            state = STATE_SUCCESS;
            try {
                value = factory.apply();
            } catch (RuntimeException e) {
                state = STATE_RUNTIME_EXCEPTION;
                throwable = e;
                throw e;
            } catch (Error e) {
                state = STATE_ERROR;
                throwable = e;
                throw e;
            } finally {
                factory = null; // allow gc, now factory is no longer needed
            }
            return value;
        case STATE_SUCCESS:
            return value;
        case STATE_RUNTIME_EXCEPTION:
            throw (RuntimeException) throwable;
        case STATE_ERROR:
            throw (Error) throwable;
        default:
            throw new AssertionError("invalid state " + state);
        }
    }
}

// End Lazy.java
