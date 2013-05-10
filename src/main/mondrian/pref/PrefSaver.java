/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2003-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.pref;

import mondrian.rolap.RolapUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Sets properties and logging levels, and remembers the original values so they
 * can be reverted at the end of the test.
 *
 * @author jhyde
 * @since Oct 28, 2008
 */
public class PrefSaver {

    public final StatementPref pref = StatementPref.instance();

    private final Map<BaseProperty, Object> originalValues =
        new HashMap<BaseProperty, Object>();

    private final Map<Logger, Level> originalLoggerLevels =
        new HashMap<Logger, Level>();

    // wacky initializer to prevent compiler from internalizing the
    // string (we don't want it to be == other occurrences of "NOT_SET")
    private static final String NOT_SET =
        new StringBuffer("NOT_" + "SET").toString();

    private void set_(BaseProperty property, Object value) {
        if (!originalValues.containsKey(property)) {
            final Object originalValue = Prefs.get(pref, property);
            originalValues.put(
                property,
                originalValue);
        }
        property.setObject(pref, value);
    }

    /**
     * Sets a boolean property and remembers its previous value.
     *
     * @param property Property
     * @param value New value
     */
    public void set(BooleanProperty property, boolean value) {
        set_(property, value);
    }

    /**
     * Sets an integer property and remembers its previous value.
     *
     * @param property Property
     * @param value New value
     */
    public void set(IntegerProperty property, int value) {
        set_(property, value);
    }

    /**
     * Sets a string property and remembers its previous value.
     *
     * @param property Property
     * @param value New value
     */
    public void set(StringProperty property, String value) {
        set_(property, value);
    }

    /**
     * Sets a double property and remembers its previous value.
     *
     * @param property Property
     * @param value New value
     */
    public void set(DoubleProperty property, Double value) {
        set_(property, value);
    }

    /**
     * Sets all properties back to their original values.
     */
    public void reset() {
        for (Map.Entry<BaseProperty, Object> entry
                 : originalValues.entrySet())
        {
            final Object value = entry.getValue();
            //noinspection StringEquality
            if (value == NOT_SET) {
                Prefs.remove(pref, entry.getKey().getPath());
            } else {
                Prefs.set(pref, entry.getKey(), value);
            }
            if (entry.getKey() == PrefDef.NullMemberRepresentation) {
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

    public void setMin(
        StatementPref pref, IntegerProperty property, int minValue)
    {
        if (pref.MaxConstraints < minValue) {
            set(property, minValue);
        }
    }
}

// End PrefSaver.java
