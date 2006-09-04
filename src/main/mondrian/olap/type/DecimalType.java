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

import mondrian.olap.Util;

/**
 * Subclass of {@link NumericType} which guarantees fixed number of decimal
 * places. In particular, a decimal with zero scale is an integer.
 *
 * @author jhyde
 * @since May 3, 2005
 * @version $Id$
 */
public class DecimalType extends NumericType {
    private final int precision;
    private final int scale;

    /**
     * Creates a decimal type with precision and scale.
     *
     * <p>Examples:<ul>
     * <li>123.45 has precision 5, scale 2.
     * <li>12,345,000 has precision 5, scale -3.
     * </ul>
     *
     * <p>The largest value is 10 ^ (precision - scale). Hence the largest
     * <code>DECIMAL(5, -3)</code> value is 10 ^ 8.
     *
     * @param precision Maximum number of decimal digits which a value of
     *   this type can have.
     *   Must be greater than zero.
     *   Use {@link Integer#MAX_VALUE} if the precision is unbounded.
     * @param scale Number of digits to the right of the decimal point.
     */
    public DecimalType(int precision, int scale) {
        super();
        Util.assertPrecondition(precision > 0, "precision > 0");
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Returns the maximum number of decimal digits which a value of
     * this type can have.
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Returns the number of digits to the right of the decimal point.
     */
    public int getScale() {
        return scale;
    }

    public String toString() {
        return precision == Integer.MAX_VALUE ?
            "DECIMAL(" + scale + ")" :
            "DECIMAL(" + precision + ", " + scale + ")";
    }
}

// End DecimalType.java
