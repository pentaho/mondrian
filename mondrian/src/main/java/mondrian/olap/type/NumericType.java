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

package mondrian.olap.type;

/**
 * The type of a numeric expression.
 *
 * @author jhyde
 * @since Feb 17, 2005
 */
public class NumericType extends ScalarType {

    /**
     * Creates a numeric type.
     */
    public NumericType() {
        this("NUMERIC");
    }

    protected NumericType(String digest) {
        super(digest);
    }

    public boolean equals(Object obj) {
        return obj instanceof NumericType
            && toString().equals(obj.toString());
    }

    public boolean isInstance(Object value) {
        return value instanceof Number
            || value instanceof Character;
    }
}

// End NumericType.java
