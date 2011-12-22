/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2009 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.olap.type;

import mondrian.olap.Dimension;
import mondrian.olap.Hierarchy;

/**
 * The type of a numeric expression.
 *
 * @author jhyde
 * @since Feb 17, 2005
 * @version $Id$
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
