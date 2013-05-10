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
 * Time: 10:18 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class BaseProperty {
    public final Scope scope;
    public final String path;
    public final Object defaultValue;
    public final String name;

    public BaseProperty(
        String name,
        Scope scope,
        String path,
        Object defaultValue)
    {
        this.name = name;
        this.scope = scope;
        this.path = path;
        this.defaultValue = defaultValue;
    }

    public String getPath() {
        return path;
    }

    public Object getObject(StatementPref pref) {
        return Prefs.get(pref, this);
    }

    protected boolean get(StatementPref pref, boolean defaultValue) {
        final Boolean value = (Boolean) Prefs.get(pref, this);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    protected int get(StatementPref pref, int defaultValue) {
        final Integer value = (Integer) Prefs.get(pref, this);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    protected double get(StatementPref pref, double defaultValue) {
        final Double value = (Double) Prefs.get(pref, this);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    protected String get(StatementPref pref, String defaultValue) {
        final String value = (String) Prefs.get(pref, this);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public void setObject(StatementPref pref, Object value) {
        Prefs.set(pref, this, value);
    }

    public abstract Settable<?> with(PrefSaver propSaver);

    public interface Settable<E> {
        void set(E e);
        E get();
    }
}

// End BaseProperty.java
