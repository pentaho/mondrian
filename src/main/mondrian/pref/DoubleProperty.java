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
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 */
public class DoubleProperty extends BaseProperty {
    private final double defaultValue;

    public DoubleProperty(
        String name, Scope scope, String path, double defaultValue)
    {
        super(name, scope, path, defaultValue);
        this.defaultValue = defaultValue;
    }

    public double get(StatementPref pref) {
        return get(pref, defaultValue);
    }

    public void set(StatementPref pref, double value) {
        setObject(pref, value);
    }

    public Settable<Double> with(final PrefSaver propSaver) {
        return new Settable<Double>() {
            public void set(Double o) {
                propSaver.set(DoubleProperty.this, o);
            }

            public Double get() {
                return (Double) Prefs.get(propSaver.pref, DoubleProperty.this);
            }
        };
    }

    public double getDefaultValue() {
        return defaultValue;
    }
}

// End DoubleProperty.java
