/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package mondrian.util;

/**
 * Type-safe value that contains an immutable string. Two instances are
 * the same if they have identical type and contain equal strings.
 */
public abstract class StringKey {
    private String value;

    /** Creates a StringKey. */
    public StringKey(String value) {
        assert value != null;
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        // Class must be identical (different subclasses of StringHolder not
        // OK).
        return obj.getClass() == getClass()
            && value.equals(((StringKey) obj).value);
    }
}

// End StringKey.java
