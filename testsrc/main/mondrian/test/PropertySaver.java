/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2003-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Feb 21, 2003
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;
import org.eigenbase.util.property.*;

import java.util.Map;
import java.util.HashMap;

/**
 * Sets properties, and remembers them so they can be reverted at the
 * end of the test.
 *
 * @author jhyde
 * @since Oct 28, 2008
 * @version $Id$
 */
public class PropertySaver {

    public final MondrianProperties properties =
        MondrianProperties.instance();

    private final Map<Property, String> originalValues =
        new HashMap<Property, String>();

    // wacky initializer to prevent compiler from internalizing the
    // string (we don't want it to be == other occurrences of "NOT_SET")
    private static final String NOT_SET =
        new StringBuffer("NOT_" + "SET").toString();

    /**
     * Sets a boolean property and remembers its previous value.
     *
     * @param property Property
     * @param value New value
     */
    public void set(BooleanProperty property, boolean value) {
        if (!originalValues.containsKey(property)) {
            final String originalValue =
                properties.containsKey(property.getPath())
                    ? properties.getProperty(property.getPath())
                    : NOT_SET;
            originalValues.put(
                property,
                originalValue);
        }
        property.set(value);
    }

    /**
     * Sets an integer property and remembers its previous value.
     *
     * @param property Property
     * @param value New value
     */
    public void set(IntegerProperty property, int value) {
        if (!originalValues.containsKey(property)) {
            final String originalValue =
                properties.containsKey(property.getPath())
                    ? properties.getProperty(property.getPath())
                    : NOT_SET;
            originalValues.put(
                property,
                originalValue);
        }
        property.set(value);
    }

    /**
     * Sets a string property and remembers its previous value.
     *
     * @param property Property
     * @param value New value
     */
    public void set(StringProperty property, String value) {
        if (!originalValues.containsKey(property)) {
            final String originalValue =
                properties.containsKey(property.getPath())
                    ? properties.getProperty(property.getPath())
                    : NOT_SET;
            originalValues.put(
                property,
                originalValue);
        }
        property.set(value);
    }

    /**
     * Sets a double property and remembers its previous value.
     *
     * @param property Property
     * @param value New value
     */
    public void set(DoubleProperty property, Double value) {
        if (!originalValues.containsKey(property)) {
            final String originalValue =
                properties.containsKey(property.getPath())
                    ? properties.getProperty(property.getPath())
                    : NOT_SET;
            originalValues.put(
                property,
                originalValue);
        }
        property.set(value);
    }

    /**
     * Sets all properties back to their original values.
     */
    public void reset() {
        for (Map.Entry<Property,String> entry : originalValues.entrySet()) {
            final String value = entry.getValue();
            //noinspection StringEquality
            if (value == NOT_SET) {
                properties.remove(entry.getKey());
            } else {
                properties.setProperty(entry.getKey().getPath(), value);
            }
        }
    }
}

// End PropertySaver.java
