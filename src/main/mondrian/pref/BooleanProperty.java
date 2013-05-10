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
 * Created with IntelliJ IDEA.
 * User: jhyde
 * Date: 5/7/13
 * Time: 10:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class BooleanProperty extends BaseProperty {
    private final boolean defaultValue;

    public BooleanProperty(
        String name, Scope scope, String path, boolean defaultValue)
    {
        super(name, scope, path, defaultValue);
        this.defaultValue = defaultValue;
    }

    public boolean get(StatementPref pref) {
        return get(pref, defaultValue);
    }

    public void set(StatementPref pref, boolean value) {
        setObject(pref, value);
    }

    public boolean getDefaultValue() {
        return defaultValue;
    }

    public Settable<Boolean> with(final PrefSaver propSaver) {
        return new Settable<Boolean>() {
            public void set(Boolean o) {
                propSaver.set(BooleanProperty.this, o);
            }

            public Boolean get() {
                return (Boolean)
                    Prefs.get(propSaver.pref, BooleanProperty.this);
            }
        };
    }
}

// End BooleanProperty.java
