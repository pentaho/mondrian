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
 * Definition of a property that has a {@link String} value.
 *
 * @see PrefDef
 */
public class StringProperty extends BaseProperty {
    private final String defaultValue;

    public StringProperty(
        String name, Scope scope, String path, String defaultValue)
    {
        super(name, scope, path, defaultValue);
        this.defaultValue = defaultValue;
    }

    public String get(StatementPref pref) {
        return get(pref, defaultValue);
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void set(StatementPref pref, String value) {
        setObject(pref, value);
    }

    /** Returns a curried object that allow get and set methods on this property
     * to be called with a given {@link PrefSaver}. */
    public Settable<String> with(final PrefSaver propSaver) {
        return new Settable<String>() {
            public void set(String o) {
                propSaver.set(StringProperty.this, o);
            }

            public String get() {
                return (String) Prefs.get(propSaver.pref, StringProperty.this);
            }
        };
    }

    @Override
    public String get(StatementPref pref, String defaultValue) {
        return super.get(pref, defaultValue);
    }
}

// End StringProperty.java
