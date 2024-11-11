/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/


package mondrian.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.eigenbase.util.property.BooleanProperty;
import org.eigenbase.util.property.DoubleProperty;
import org.eigenbase.util.property.IntegerProperty;
import org.eigenbase.util.property.Property;
import org.eigenbase.util.property.StringProperty;

import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.rolap.RolapUtil;

/**
 * Sets properties and logging levels, and remembers the original values so they
 * can be reverted at the end of the test.
 *
 * @author jhyde
 * @since Oct 28, 2008
 */
public class PropertySaver {

    public final MondrianProperties properties =
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
        for (Map.Entry<Property, String> entry : originalValues.entrySet()) {
            final String value = entry.getValue();
            //noinspection StringEquality
            if (value == NOT_SET) {
                properties.remove(entry.getKey().getPath());
            } else {
                properties.setProperty(entry.getKey().getPath(), value);
            }
            if (entry.getKey()
                == MondrianProperties.instance().NullMemberRepresentation)
            {
                RolapUtil.reloadNullLiteral();
            }
        }
        for (Map.Entry<Logger, Level> entry : originalLoggerLevels.entrySet()) {
            Util.setLevel( entry.getKey() , entry.getValue() );
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
        Util.setLevel( logger, level );
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
            || !(prevLevel.compareTo(level) <= 0))
        {
            set(logger, level);
        }
    }
}

// End PropertySaver.java
