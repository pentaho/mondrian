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


package mondrian.gui.validate;

/**
 * Message provider. Extracted interface from <code>mondrian.gui.I18n</code>.
 *
 * @author mlowery
 */
public interface Messages {
    /**
     * Returns the string with given key.
     *
     * @param stringId key
     * @param defaultValue default if key does not exist
     * @return message
     */
    String getString(
        String stringId,
        String defaultValue);

    /**
     * Returns the string with given key with substitutions.
     *
     * @param stringId Key
     * @param defaultValue default if key does not exist
     * @param args arguments to substitute
     * @return message
     */
    String getFormattedString(
        String stringId,
        String defaultValue,
        Object... args);
}

// End Messages.java
