/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2001-2005 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 22 December, 2002
*/
package mondrian.util;

import java.util.Properties;

/**
 * <code>PropertiesPlus</code> adds a couple of convenience methods to
 * {@link java.util.Properties}.
 *
 * @author jhyde
 * @since 22 December, 2002
 * @version $Id$
 **/
public class PropertiesPlus extends Properties {
    /**
     * Retrieves an integer property. Returns -1 if the property is not
     * found, or if its value is not an integer.
     */
    public int getIntProperty(String key) {
        return getIntProperty(key, -1);
    }
    /**
     * Retrieves an integer property. Returns <code>default</code> if the
     * property is not found.
     */
    public int getIntProperty(String key, int defaultValue) {
        String value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        int i = Integer.valueOf(value).intValue();
        return i;
    }
    /**
     * Retrieves a double-precision property. Returns <code>default</code> if
     * the property is not found.
     */
    public double getDoubleProperty(String key, double defaultValue) {
        String value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        double d = Double.valueOf(value).doubleValue();
        return d;
    }
    /**
     * Retrieves a boolean property. Returns <code>true</code> if the
     * property exists, and its value is <code>1</code>, <code>true</code>
     * or <code>yes</code>; returns <code>false</code> otherwise.
     */
    public boolean getBooleanProperty(String key) {
        String value = getProperty(key);
        if (value == null) {
            return false;
        }
        return value.equalsIgnoreCase("1") ||
            value.equalsIgnoreCase("true") ||
            value.equalsIgnoreCase("yes");
    }
}

// End PropertiesPlus.java
