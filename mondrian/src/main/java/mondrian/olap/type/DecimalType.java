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

import mondrian.olap.Util;

/**
 * Subclass of {@link NumericType} which guarantees fixed number of decimal
 * places. In particular, a decimal with zero scale is an integer.
 *
 * @author jhyde
 * @since May 3, 2005
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
        super(
            precision == Integer.MAX_VALUE
                ? "DecimalType(" + scale + ")"
                : "DecimalType(" + precision + ", " + scale + ")");
        Util.assertPrecondition(precision > 0, "precision > 0");
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Returns the maximum number of decimal digits which a value of
     * this type can have.
     *
     * @return precision of this type
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Returns the number of digits to the right of the decimal point.
     *
     * @return scale of this type
     */
    public int getScale() {
        return scale;
    }

    public boolean equals(Object obj) {
        if (obj instanceof DecimalType) {
            DecimalType that = (DecimalType) obj;
            return this.precision == that.precision
                && this.scale == that.scale;
        }
        return false;
    }
}

// End DecimalType.java
