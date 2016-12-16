/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2005 Julian Hyde
// Copyright (C) 2005-2009 Pentaho
// All Rights Reserved.
*/

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
