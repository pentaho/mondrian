/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2013-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.pref;

/**
 * Definition of a property that has an {@code int} value.
 *
 * @see PrefDef
 */
public class IntegerProperty extends BaseProperty {
    private final int defaultValue;

    public IntegerProperty(
        String name, Scope scope, String path, int defaultValue)
    {
        super(name, scope, path, defaultValue);
        this.defaultValue = defaultValue;
    }

    public int get(StatementPref pref) {
        return get(pref, defaultValue);
    }

    public void set(StatementPref pref, int value) {
        setObject(pref, value);
    }

    @Override
    public Settable<Integer> with(final PrefSaver propSaver) {
        return new Settable<Integer>() {
            public void set(Integer o) {
                propSaver.set(IntegerProperty.this, o);
            }

            public Integer get() {
                return (Integer)
                    Prefs.get(propSaver.pref, IntegerProperty.this);
            }
        };
    }

    public int getDefaultValue() {
        return defaultValue;
    }
}

// End IntegerProperty.java
