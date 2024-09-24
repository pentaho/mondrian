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
package mondrian.rolap.format;

import java.math.BigDecimal;

/**
 * Default formatter which can be used for different rolap member properties.
 */
class DefaultFormatter {

    /**
     * Numbers are a special case. We don't want any
     * scientific notations, as well as inaccurate decimal values.
     * So we wrap in a BigDecimal, and format before calling toString.
     *
     * @param value generic value to be formatted
     * @return formatted value
     */
    String format(Object value) {
        if (value != null) {
            if (value instanceof Number) {
                BigDecimal numberValue =
                        new BigDecimal(value.toString()).stripTrailingZeros();

                // Can be removed if running on Java 8.
                // That's an old bug, and now it's closed:
                // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6480539
                if (BigDecimal.ZERO.compareTo(numberValue) == 0) {
                    numberValue = BigDecimal.ZERO;
                }

                return numberValue.toPlainString();
            }
            return value.toString();
        }
        return null;
    }
}
// End DefaultFormatter.java