/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2005 Julian Hyde
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
    }

    public String toString() {
        return "NUMERIC";
    }
}

// End NumericType.java
