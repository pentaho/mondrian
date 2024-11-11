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
 * The type of a boolean expression.
 *
 * @author jhyde
 * @since Feb 17, 2005
 */
public class BooleanType extends ScalarType {
    /**
     * Creates a BooleanType.
     */
    public BooleanType() {
        super("BOOLEAN");
    }

    public boolean equals(Object obj) {
        return obj instanceof BooleanType;
    }

    public boolean isInstance(Object value) {
        return value instanceof Boolean;
    }
}

// End BooleanType.java
