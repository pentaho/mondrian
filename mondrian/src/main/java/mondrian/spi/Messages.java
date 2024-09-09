/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package mondrian.spi;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Support for localized messages.
 */
public class Messages {

    private static final String BUNDLE_NAME = "com.mysql.cj.LocalizedErrorMessages";

    private static final ResourceBundle RESOURCE_BUNDLE;
    private static final Object[] emptyObjectArray = {};

    static {
        ResourceBundle temp = null;

        //
        // Overly-pedantic here, some appserver and JVM combos don't deal well with the no-args version, others don't deal well with the three-arg version, so
        // we need to try both :(
        //

        try {
            temp = ResourceBundle.getBundle(BUNDLE_NAME, Locale.getDefault(), Messages.class.getClassLoader());
        } catch (Throwable t) {
            try {
                temp = ResourceBundle.getBundle(BUNDLE_NAME);
            } catch (Throwable t2) {
                RuntimeException rt = new RuntimeException("Can't load resource bundle due to underlying exception " + t.toString());
                rt.initCause(t2);

                throw rt;
            }
        } finally {
            RESOURCE_BUNDLE = temp;
        }
    }

    /**
     * Returns the localized message for the given message key
     *
     * @param key
     *            the message key
     * @return The localized message for the key
     */
    public static String getString(String key) {
        return getString(key, emptyObjectArray);
    }

    public static String getString(String key, Object[] args) {
        if (RESOURCE_BUNDLE == null) {
            throw new RuntimeException("Localized messages from resource bundle '" + BUNDLE_NAME + "' not loaded during initialization of driver.");
        }

        try {
            if (key == null) {
                throw new IllegalArgumentException("Message key can not be null");
            }

            String message = RESOURCE_BUNDLE.getString(key);

            if (message == null) {
                message = "Missing error message for key '" + key + "'";
            }

            return MessageFormat.format(message, args);
        } catch (MissingResourceException e) {
            return '!' + key + '!';
        }
    }

    /**
     * Dis-allow construction ...
     */
    private Messages() {
    }

}