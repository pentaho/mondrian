/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2012 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.MondrianProperties;
import mondrian.rolap.RolapUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.eigenbase.util.property.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Sets properties and logging levels, and remembers the original values so they
 * can be reverted at the end of the test.
 *
 * @author jhyde
 * @since Oct 28, 2008
 */
public class PropertySaver {

    public final MondrianProperties props =
        MondrianProperties.instance();

    private final Map<Property, String> originalValues =
        new HashMap<Property, String>();

    private final Map<Logger, Level> originalLoggerLevels =
        new HashMap<Logger, Level>();

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
                props.containsKey(property.getPath())
                    ? props.getProperty(property.getPath())
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
                props.containsKey(property.getPath())
                    ? props.getProperty(property.getPath())
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
                props.containsKey(property.getPath())
                    ? props.getProperty(property.getPath())
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
                props.containsKey(property.getPath())
                    ? props.getProperty(property.getPath())
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
        for (Map.Entry<Property, String> entry : originalValues.entrySet()) {
            final String value = entry.getValue();
            //noinspection StringEquality
            if (value == NOT_SET) {
                props.remove(entry.getKey().getPath());
            } else {
                entry.getKey().setString(value);
            }
            if (entry.getKey()
                == MondrianProperties.instance().NullMemberRepresentation)
            {
                RolapUtil.reloadNullLiteral();
            }
        }
        for (Map.Entry<Logger, Level> entry : originalLoggerLevels.entrySet()) {
            entry.getKey().setLevel(entry.getValue());
        }
    }

    /**
     * Sets a logger's level.
     *
     * @param logger Logger
     * @param level Logging level
     */
    public void set(Logger logger, Level level) {
        final Level prevLevel = logger.getLevel();
        if (!originalLoggerLevels.containsKey(logger)) {
            originalLoggerLevels.put(logger, prevLevel);
        }
        logger.setLevel(level);
    }

    /**
     * Sets a logger's level to at least the given level.
     *
     * @param logger Logger
     * @param level Logging level
     */
    public void setAtLeast(Logger logger, Level level) {
        final Level prevLevel = logger.getLevel();
        if (prevLevel == null
            || !prevLevel.isGreaterOrEqual(level))
        {
            set(logger, level);
        }
    }
}

// End PropertySaver.java
